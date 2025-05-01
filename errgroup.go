package syro

import "strings"

// ErrGroup is a helper struct for cases when a single function
// could have multiple errors which should be accumulated,
// instead of returning the first one.
type ErrGroup []error

// TODO: newline and identifiers?

type ErrGroupProps struct {
	ID          string
	WithNewline bool
}

func NewErrGroup(ep ...ErrGroupProps) *ErrGroup {
	return &ErrGroup{}
}

func (eg *ErrGroup) Add(err error) {
	if eg != nil && err != nil {
		*eg = append(*eg, err)
	}
}

func (eg *ErrGroup) Errors() []error {
	if eg != nil && len(*eg) > 0 {
		return *eg
	}
	return nil
}

// Error implements the error interface. It returns a concatenated string of all
// non-nil ErrGroup, each separated by a semicolon.
func (eg *ErrGroup) Error() string {
	if eg == nil {
		return ""
	}

	var errs []string
	for _, err := range *eg {
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	return strings.Join(errs, "; ")
}

func (eg *ErrGroup) Len() int {
	if eg == nil {
		return 0
	}

	return len(*eg)
}

// Return the error only if at least one of them happened. This is done because
// the ErrGroup is not nil when created, but it may be empty.
func (eg *ErrGroup) ToErr() error {
	if eg == nil {
		return nil
	}

	if eg.Len() == 0 {
		return nil
	}

	return eg
}
