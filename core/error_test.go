package core

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
)

func TestDBError(t *testing.T) {
	// 测试NewDBError和Error方法
	baseErr := errors.New("base error")
	dbErr := NewDBError(FunctionModelCoreDataConversion, ErrorTypeSystem, ErrorBaseCodeInnerParameterError, baseErr)
	if dbErr.Error() != baseErr.Error() {
		t.Errorf("DBError.Error failed, expected %q but got %q", baseErr.Error(), dbErr.Error())
	}

	// 测试GetErrorCode方法
	expectedCode := string(FunctionModelCoreDataConversion) + string(ErrorTypeSystem) + string(ErrorBaseCodeInnerParameterError)
	if dbErr.GetErrorCode() != expectedCode {
		t.Errorf("DBError.GetErrorCode failed, expected %q but got %q", expectedCode, dbErr.GetErrorCode())
	}

	// 测试Model方法
	if dbErr.Model() != FunctionModelCoreDataConversion {
		t.Errorf("DBError.Model failed, expected %q but got %q", FunctionModelCoreDataConversion, dbErr.Model())
	}

	// 测试GetErrorType方法
	if dbErr.GetErrorType() != ErrorTypeSystem {
		t.Errorf("DBError.GetErrorType failed, expected %q but got %q", ErrorTypeSystem, dbErr.GetErrorType())
	}

	// 测试PrintError方法
	buf := bytes.Buffer{}
	log.SetOutput(&buf)
	dbErr.PrintError()
	loggedMsg := strings.TrimSpace(buf.String())
	expectedMsg := fmt.Sprintf("Error: %s, 原始错误: %s", ErrorTypeSystem, baseErr.Error())
	if loggedMsg != expectedMsg {
		t.Errorf("DBError.PrintError failed, expected %q but got %q", expectedMsg, loggedMsg)
	}
}
