package pprofhandler

import (
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/gorilla/mux"

	"github.com/goverland-labs/analytics-service/pkg/middleware"
)

const readHeaderTimeout = 30 * time.Second

func NewPprofServer(listen string) *http.Server {
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)

	router := mux.NewRouter()
	router.Use(middleware.Panic)

	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := &http.Server{
		Addr:              listen,
		Handler:           router,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	return server
}
