package dmap

import (
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
)

func (s *Service) functionOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getOrCreateDMap(req.DMap())
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	e := &env{
		opcode:    protocol.OpFunction,
		dmap:      req.DMap(),
		key:       req.Key(),
		value:     req.Value(),
		timestamp: time.Now().UnixNano(),
		kind:      partitions.PRIMARY,
	}
	extra := req.Extra()
	if extra != nil {
		e.function = extra.(protocol.FunctionExtra).Function
	}

	result, err := dm.function(e)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetValue(result)
	w.SetStatus(protocol.StatusOK)
}
