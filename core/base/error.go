package base

import (
	"fmt"

	"ne_database/utils"
)

type StandardError interface {
	error
	Model() FunctionModel
	GetErrorType() ErrorType
	GetErrorCode() string
	PrintError()
}

type DBError struct {
	error
	baseError   error
	model       FunctionModel
	errType     ErrorType
	errBaseCode ErrorBaseCode
}

func (e *DBError) Model() FunctionModel {
	return e.model
}

func (e *DBError) GetErrorType() ErrorType {
	return e.errType
}

func (e *DBError) GetErrorCode() string {
	return string(e.model) + " " + string(e.errType) + " " + string(e.errBaseCode)
}

func (e *DBError) PrintError() {
	utils.LogError(fmt.Sprintf("Error: %s, 原始错误: %s", e.GetErrorCode(), e.baseError.Error()))
}

func (e *DBError) Error() string {
	// 这里还是返回原始错误的error信息
	return e.baseError.Error()
}

func NewDBError(model FunctionModel, errType ErrorType, errBaseCode ErrorBaseCode, baseError error) *DBError {
	return &DBError{
		model:       model,
		errType:     errType,
		errBaseCode: errBaseCode,
		baseError:   baseError,
	}
}
