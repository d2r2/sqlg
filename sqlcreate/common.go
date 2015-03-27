package sqlcreate

import (
	"fmt"
	"github.com/d2r2/sqlg/logger"
)

var f = fmt.Sprintf
var e = fmt.Errorf
var log = logger.NewLogger(
	//    VL_DEBUG,
	logger.VL_INFO,
	"sqlcreate",
	true)
