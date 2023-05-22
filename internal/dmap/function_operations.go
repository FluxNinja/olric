package dmap

import (
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
)

func (s *Service) functionOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getDMap(req.DMap())
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	e := &env{
		opcode:    protocol.OpFunction,
		dmap:      req.DMap(),
		key:       req.Key(),
		value:     req.Value(),
		function:  req.Function(),
		timestamp: time.Now().UnixNano(),
		kind:      partitions.PRIMARY,
	}
	extra := req.Extra()
	if extra != nil {
		e.timestamp = extra.(protocol.FunctionExtra).Timestamp
	}

	result, err := dm.function(e)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetValue(result)
	w.SetStatus(protocol.StatusOK)
}
