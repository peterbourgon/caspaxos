package httpapi

import "net/http"

type interceptingWriter struct {
	http.ResponseWriter
	code int
}

func (iw *interceptingWriter) WriteHeader(code int) {
	iw.code = code
	iw.ResponseWriter.WriteHeader(code)
}
