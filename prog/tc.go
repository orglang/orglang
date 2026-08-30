package prog

import (
	"github.com/alecthomas/participle/v2"
)

func msgFromText(text string) (SpecME, error) {
	parser, buildErr := participle.Build[SpecME]()
	if buildErr != nil {
		return SpecME{}, buildErr
	}
	spec, parseErr := parser.ParseString("", text)
	if parseErr != nil {
		return SpecME{}, buildErr
	}
	return *spec, nil
}
