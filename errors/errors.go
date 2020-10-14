package errors

import (
	"strings"
)

type ErrorList []error

func NewErrorList(errors ...error) error {
	var errs ErrorList
	for _, err := range errors {
		if err != error(nil) {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errs
}

func (e ErrorList) Error() string {
	var errs []string
	for i, err := range e {
		errs[i] = err.Error()
	}
	return strings.Join(errs, "\n")
}
