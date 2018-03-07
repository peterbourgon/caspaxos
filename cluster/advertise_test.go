package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/go-kit/kit/log"
)

func TestCalculateAdvertiseAddr(t *testing.T) {
	r := mockResolver{
		"validhost.com": []net.IPAddr{{IP: net.ParseIP("10.21.32.43")}},
		"multihost.com": []net.IPAddr{{IP: net.ParseIP("10.1.0.1")}, {IP: net.ParseIP("10.1.0.2")}},
	}
	for _, testcase := range []struct {
		name          string
		bindAddr      string
		advertiseAddr string
		want          string
	}{
		{"Public bind no advertise",
			"1.2.3.4", "", "1.2.3.4",
		},
		{"Private bind no advertise",
			"10.1.2.3", "", "10.1.2.3",
		},
		{"Zeroes bind public advertise",
			"0.0.0.0", "2.3.4.5", "2.3.4.5",
		},
		{"Zeroes bind private advertise",
			"0.0.0.0", "172.16.1.9", "172.16.1.9",
		},
		{"Public bind private advertise",
			"188.177.166.155", "10.11.12.13", "10.11.12.13",
		},
		{"IPv6 bind no advertise",
			"::", "", "::",
		},
		{"IPv6 bind private advertise",
			"::", "172.16.1.1", "172.16.1.1",
		},
		{"Valid hostname as bind addr",
			"validhost.com", "", "10.21.32.43",
		},
		{"Valid hostname as advertise addr",
			"0.0.0.0", "validhost.com", "10.21.32.43",
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			ip, err := calculateAdvertiseIP(testcase.bindAddr, testcase.advertiseAddr, r, log.NewNopLogger())
			if err != nil {
				t.Fatal(err)
			}
			if want, have := testcase.want, ip.String(); want != have {
				t.Fatalf("want '%s', have '%s'", want, have)
			}
		})
	}
}

func TestHasNonlocal(t *testing.T) {
	for _, testcase := range []struct {
		name      string
		hostports []string
		want      bool
	}{
		{"empty",
			[]string{}, false,
		},
		{"localhost only",
			[]string{"localhost"}, false,
		},
		{"127 only",
			[]string{"127.0.0.1", "127.1.2.3"}, false,
		},
		{"10s",
			[]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, true,
		},
		{"hostnames",
			[]string{"foo.example.com", "bar.example.com"}, true,
		},
		{"mix",
			[]string{"localhost", "foo", "127.0.0.1"}, true,
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			if want, have := testcase.want, hasNonlocal(testcase.hostports); want != have {
				t.Fatalf("%v: want %v, have %v", testcase.hostports, want, have)
			}
		})
	}
}

func TestIsUnroutable(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1":       true,
		"127.0.0.1:1234":  true,
		"localhost":       true,
		"0.0.0.0":         true,
		"::1":             true,
		"":                false,
		"10.0.0.1":        false,
		"192.168.1.1":     false,
		"foo.example.com": false,
		"foo":             false,
	} {
		t.Run(fmt.Sprintf("%q", addr), func(t *testing.T) {
			if have := isUnroutable(addr); want != have {
				t.Fatalf("want %v, have %v", want, have)
			}
		})
	}
}

type mockResolver map[string][]net.IPAddr

func (r mockResolver) LookupIPAddr(_ context.Context, address string) ([]net.IPAddr, error) {
	if ips, ok := r[address]; ok {
		return ips, nil
	}
	return nil, fmt.Errorf("address not found")
}
