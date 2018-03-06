package httpapi

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/peterbourgon/caspaxos/protocol"
)

// versionValue is used for CAS writes. We use encoding.BinaryMarshaler to get
// it into and out of []byte internally. The user interacts with a nicer API.
type versionValue struct {
	version uint64
	value   []byte
}

func (vv *versionValue) MarshalBinary() ([]byte, error) {
	data := make([]byte, (64/8)+len(vv.value))
	binary.LittleEndian.PutUint64(data, vv.version)
	copy(data[8:], vv.value)
	return data, nil
}

func (vv *versionValue) UnmarshalBinary(data []byte) error {
	if len(data) < (64 / 8) {
		return errors.New("data source not large enough")
	}
	vv.version = binary.LittleEndian.Uint64(data[:8])
	vv.value = make([]byte, len(data)-8)
	copy(vv.value, data[8:])
	return nil
}

func read(x []byte) []byte { return x }

// From the paper: "To update a register to val_1 if the current version is 5,
// [we can use] x -> if x = (5,*) then (6,val_1) else x."
func cas(version uint64, value []byte) protocol.ChangeFunc {
	return func(x []byte) (res []byte) {
		var vv versionValue
		if x != nil {
			if err := vv.UnmarshalBinary(x); err != nil {
				panic(fmt.Sprintf("attempt to unmarshal version/value tuple failed: %v", err))
			}
		}

		if vv.version != version {
			return x // no dice
		}

		vv.version++
		vv.value = value
		newState, err := vv.MarshalBinary()
		if err != nil {
			panic(fmt.Sprintf("attempt to marshal version/value tuple failed: %v", err))
		}

		return newState
	}
}

// ProposerServer wraps a protocol.Proposer and provides a basic HTTP API.
// Note that this is an artificially restricted API. The protocol itself
// is much more expressive, this is mostly meant as a tech demo.
//
//     GET /{key}
//         Returns the current version and value for key.
//         Returns 404 Not Found if the key doesn't exist.
//
//     POST /{key}?version={version}&value={value}
//         Performs a compare-and-swap, writing value to key.
//         Version should be the current version of the key.
//         Returns 412 Precondition Failed on version error.
//
//     DELETE /{key}?version={version}
//         Deletes the key, if the version matches.
//         Returns 404 Not Found if the key doesn't exist.
//         Returns 412 Precondition Failed on version error.
//
type ProposerServer struct {
	http.Handler
	proposer protocol.Proposer
	logger   log.Logger
}

// NewProposerServer returns an ProposerServer wrapping the provided proposer.
// The ProposerServer is an http.Handler and can ServeHTTP.
func NewProposerServer(proposer protocol.Proposer, logger log.Logger) ProposerServer {
	ps := ProposerServer{
		proposer: proposer,
		logger:   logger,
	}
	{
		r := mux.NewRouter().StrictSlash(true)
		r.Methods("GET").Path("/{key}").HandlerFunc(ps.handleGet)
		r.Methods("POST").Path("/{key}").HandlerFunc(ps.handlePost)
		r.Methods("DELETE").Path("/{key}").HandlerFunc(ps.handleDelete)
		r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Proposer encountered unrecognized request path: "+r.URL.String(), http.StatusNotFound)
		})
		ps.Handler = r
	}
	return ps
}

func (ps ProposerServer) handleGet(w http.ResponseWriter, r *http.Request) {
	iw := &interceptingWriter{w, http.StatusOK}
	defer func(begin time.Time) {
		level.Info(ps.logger).Log(
			"handler", "handleGet",
			"method", r.Method,
			"url", r.URL.String(),
			"took", time.Since(begin),
			"status", iw.code,
		)
	}(time.Now())
	w = iw

	key := mux.Vars(r)["key"]
	if key == "" {
		respondErrorf(w, http.StatusBadRequest, "no key provided")
		return
	}

	returnedValue, _, err := ps.proposer.Propose(r.Context(), key, read)
	if err != nil {
		respondError(w, http.StatusInternalServerError, errors.Wrap(err, "Propose failed"))
		return
	}

	if returnedValue == nil {
		respondErrorf(w, http.StatusNotFound, "key %s doesn't exist", key)
		return
	}

	var vv versionValue
	if err := vv.UnmarshalBinary(returnedValue); err != nil {
		respondError(w, http.StatusInternalServerError, errors.Wrap(err, "error decoding returned state"))
		return
	}

	respondSuccess(w, map[string]interface{}{
		"key":     key,
		"version": vv.version,
		"value":   string(prettyPrint(vv.value)),
	})
}

func (ps ProposerServer) handlePost(w http.ResponseWriter, r *http.Request) {
	iw := &interceptingWriter{w, http.StatusOK}
	defer func(begin time.Time) {
		level.Info(ps.logger).Log(
			"handler", "handlePost",
			"method", r.Method,
			"url", r.URL.String(),
			"took", time.Since(begin),
			"status", iw.code,
		)
	}(time.Now())
	w = iw

	vars := mux.Vars(r)
	key := vars["key"]
	if key == "" {
		respondErrorf(w, http.StatusBadRequest, "no key specified")
		return
	}

	var version uint64
	if s := r.URL.Query().Get("version"); s != "" {
		var err error
		version, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, errors.Wrap(err, "error parsing version"))
			return
		}
	}

	var value []byte
	s := r.URL.Query().Get("value")
	if s == "" {
		respondErrorf(w, http.StatusBadRequest, "no value provided")
		return
	}
	value = []byte(s)

	returnedValue, _, err := ps.proposer.Propose(r.Context(), key, cas(version, value))
	if err != nil {
		respondError(w, http.StatusInternalServerError, errors.Wrap(err, "Propose failed"))
		return
	}

	var vv versionValue
	if returnedValue != nil {
		if err := vv.UnmarshalBinary(returnedValue); err != nil {
			respondError(w, http.StatusInternalServerError, errors.Wrap(err, "error decoding returned state"))
			return
		}
	}

	response := map[string]interface{}{
		"provided": map[string]interface{}{
			"key":     key,
			"version": version,
			"value":   string(prettyPrint(value)),
		},
		"returned": map[string]interface{}{
			"key":     key,
			"version": vv.version,
			"value":   string(prettyPrint(vv.value)),
		},
	}

	if bytes.Compare(vv.value, value) != 0 { // CAS failure
		response["error"] = "compare-and-swap failure; likely version incompatibility"
		respondErrorMap(w, http.StatusPreconditionFailed, response)
		return
	}

	respondSuccess(w, response)
}

func (ps ProposerServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	iw := &interceptingWriter{w, http.StatusOK}
	defer func(begin time.Time) {
		level.Info(ps.logger).Log(
			"handler", "handlePost",
			"method", r.Method,
			"url", r.URL.String(),
			"took", time.Since(begin),
			"status", iw.code,
		)
	}(time.Now())
	w = iw

	vars := mux.Vars(r)
	key := vars["key"]
	if key == "" {
		respondErrorf(w, http.StatusBadRequest, "no key specified")
		return
	}

	s := r.URL.Query().Get("version")
	if s == "" {
		respondErrorf(w, http.StatusBadRequest, "no version provided")
	}

	version, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		respondError(w, http.StatusBadRequest, errors.Wrap(err, "error parsing version"))
		return
	}

	respondErrorf(w, http.StatusNotImplemented, "delete key %s version %d not yet implemented", key, version)
}

func respondSuccess(w http.ResponseWriter, m map[string]interface{}) {
	respondJSON(w, http.StatusOK, m)
}

func respondError(w http.ResponseWriter, code int, err error) {
	respondErrorMap(w, code, map[string]interface{}{"error": err.Error()})
}

func respondErrorf(w http.ResponseWriter, code int, format string, args ...interface{}) {
	respondError(w, code, fmt.Errorf(format, args...))
}

func respondErrorMap(w http.ResponseWriter, code int, m map[string]interface{}) {
	m["status"] = fmt.Sprintf("%d %s", code, http.StatusText(code))
	respondJSON(w, code, m)
}

func respondJSON(w http.ResponseWriter, code int, m map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	buf, _ := json.MarshalIndent(m, "", "    ")
	w.Write(buf)
}

type prettyPrint []byte

func (pp prettyPrint) String() string {
	if pp == nil {
		return "Ø"
	}
	return string(pp)
}
