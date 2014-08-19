package sqlg

import (
	"fmt"
)

var f = fmt.Sprintf
var e = fmt.Errorf
var log = NewLogger(
	//    VL_DEBUG,
	VL_INFO,
	"sqlg",
	true)
