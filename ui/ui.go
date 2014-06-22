package ui

import (
	"errors"
	"html/template"
	"io"
	"net"
	"net/http"

	"github.com/meeko/meekod/broker/services/rpc"
)

type UI struct {
	listener net.Listener
	rpc      *rpcMiddleware
}

func (web *UI) WrapRpcExchange(exchange rpc.Exchange) rpc.Exchange {
	web.rpc = newRpcMiddleware(exchange)
	return web.rpc
}

func (web *UI) ListenAndServe(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/services/rpc", web.handleServicesRPC)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	web.listener = listener

	return http.Serve(listener, mux)
}

func (web *UI) Close() error {
	if web.listener != nil {
		listener := web.listener
		web.listener = nil
		return listener.Close()
	}
	return errors.New("not listening")
}

func (web *UI) handleServicesRPC(w http.ResponseWriter, r *http.Request) {
	if web.rpc == nil {
		noData(w)
		return
	}

	t, err := template.New("rpcService").Parse(`
<html>
  <body>
    <table>
	{{range .}}<tr><td>{{.}}</td></tr>{{end}}
	</table>
  </body>
</html>
	`)
	if err != nil {
		http.Error(w, "500 Failed to Parse the Template", http.StatusInternalServerError)
		return
	}

	if err = t.Execute(w, web.rpc.agents); err != nil {
		http.Error(w, "500 Internal Server Error", http.StatusInternalServerError)
	}
}

func noData(w http.ResponseWriter) {
	io.WriteString(w, `
<html>
  <body>
    No data available, sorry.
  </body>
</html>
	`)
}
