package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"time"

	logging "github.com/ipfs/go-log/v2"
	finderhttpclient "github.com/ipni/storetheindex/api/v0/finder/client/http"
	"github.com/ischasny/dhfind/metrics"
)

// preferJSON specifies weather to prefer JSON over NDJSON response when request accepts */*, i.e.
// any response format, has no `Accept` header at all.
const preferJSON = true

// simulationWorkerCount is a number of background workers that find tasks are delegated to in simulation mode
const simulationWorkerCount = 50

var (
	logger = logging.Logger("server")
)

type Server struct {
	s                 *http.Server
	m                 *metrics.Metrics
	c                 *finderhttpclient.DHashClient
	simulation        bool
	simulationJobs    chan *http.Request
	simulationCancel  context.CancelFunc
	simulationContext context.Context
}

// responseWriterWithStatus is required to capture status code from ResponseWriter so that it can be reported
// to metrics in a unified way
type responseWriterWithStatus struct {
	http.ResponseWriter
	status int
	// TODO: remove that once the server is run in non-simulaiton mode
	header http.Header
}

func newResponseWriterWithStatus(w http.ResponseWriter) *responseWriterWithStatus {
	ws := &responseWriterWithStatus{
		ResponseWriter: w,
		// 200 status should be assumed by default if WriteHeader hasn't been called explicitly https://pkg.go.dev/net/http#ResponseWriter
		status: 200,
	}
	if w == nil {
		ws.header = make(http.Header)
	}
	return ws
}

func (rec *responseWriterWithStatus) Header() http.Header {
	if rec.ResponseWriter != nil {
		return rec.Header()
	}
	return rec.header
}

func (rec *responseWriterWithStatus) Write(b []byte) (int, error) {
	if rec.ResponseWriter != nil {
		return rec.Write(b)
	}
	return len(b), nil
}

func (rec *responseWriterWithStatus) WriteHeader(code int) {
	rec.status = code
	if rec.ResponseWriter != nil {
		rec.ResponseWriter.WriteHeader(code)
	}
}

func New(ctx context.Context, addr, dhaddr string, m *metrics.Metrics, simulation bool) (*Server, error) {
	var server Server
	var err error

	server.s = &http.Server{
		Addr:    addr,
		Handler: server.serveMux(),
	}

	server.c, err = finderhttpclient.NewDHashClient(dhaddr, dhaddr)
	if err != nil {
		return nil, err
	}

	server.m = m

	server.simulation = simulation

	if simulation {
		server.simulationJobs = make(chan *http.Request, simulationWorkerCount)
		server.simulationContext, server.simulationCancel = context.WithCancel(ctx)
	}

	return &server, nil
}

func (s *Server) serveMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/multihash/", s.handleMhSubtree)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/", s.handleCatchAll)
	return mux
}

func (s *Server) Start(_ context.Context) error {
	ln, err := net.Listen("tcp", s.s.Addr)
	if err != nil {
		return err
	}
	go func() { _ = s.s.Serve(ln) }()

	for i := 1; i <= simulationWorkerCount; i++ {
		go s.simulationWorker()
	}

	logger.Infow("Server started", "addr", ln.Addr())
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.simulationCancel()
	return s.s.Shutdown(ctx)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ws := newResponseWriterWithStatus(w)
	defer s.reportLatency(start, ws.status, r.Method, "ready")
	discardBody(r)
	switch r.Method {
	case http.MethodGet:
		ws.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "", http.StatusNotFound)
	}
}

func (s *Server) handleMhSubtree(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	switch r.Method {
	case http.MethodGet:
		if s.simulation {
			s.simulationJobs <- r
			http.Error(w, "", http.StatusNotFound)
		} else {
			ws := newResponseWriterWithStatus(w)
			defer s.reportLatency(start, ws.status, r.Method, "multihash")
			s.handleGetMh(newIPNILookupResponseWriter(ws, preferJSON), r)
		}
	default:
		discardBody(r)
		http.Error(w, "", http.StatusNotFound)
	}
}

func (s *Server) handleGetMh(w lookupResponseWriter, r *http.Request) {
	if err := w.Accept(r); err != nil {
		switch e := err.(type) {
		case errHttpResponse:
			e.WriteTo(w)
		default:
			logger.Errorw("Failed to accept lookup request", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
		}
		return
	}
	ctx := context.Background()

	findResponse, err := s.c.Find(ctx, w.Key())

	if err != nil {
		s.handleError(w, err)
		return
	}

	// There going to be exactly one item in the array as we seached for one multihash only
	// and if it wasn't found then an error would have been returned
	mhr := findResponse.MultihashResults[0]

	for _, pr := range mhr.ProviderResults {
		if err := w.WriteProviderResult(pr); err != nil {
			logger.Errorw("Failed to encode provider result", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
	}
	if err := w.Close(); err != nil {
		switch e := err.(type) {
		case errHttpResponse:
			e.WriteTo(w)
		default:
			logger.Errorw("Failed to finalize lookup results", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
		}
	}
}

func (s *Server) reportLatency(start time.Time, status int, method, path string) {
	s.m.RecordHttpLatency(context.Background(), time.Since(start), method, path, status)
}

func discardBody(r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	_ = r.Body.Close()
}

func (s *Server) handleCatchAll(w http.ResponseWriter, r *http.Request) {
	discardBody(r)
	http.Error(w, "", http.StatusNotFound)
}

func (s *Server) handleError(w http.ResponseWriter, err error) {
	var status int
	switch err.(type) {
	case ErrUnsupportedMulticodecCode, ErrMultihashDecode:
		status = http.StatusBadRequest
	default:
		status = http.StatusInternalServerError
	}
	http.Error(w, err.Error(), status)
}

func (s *Server) simulationWorker() {
	select {
	case <-s.simulationContext.Done():
		logger.Info("Finished")
		return
	case job := <-s.simulationJobs:
		var ws *responseWriterWithStatus
		start := time.Now()
		ws = newResponseWriterWithStatus(nil)
		if job.Method == http.MethodGet {
			s.handleGetMh(newIPNILookupResponseWriter(ws, preferJSON), job)
			s.reportLatency(start, ws.status, job.Method, "multihash")
		}
	}
}
