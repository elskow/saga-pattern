package validate

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

func DecodeStrictJSON(r io.Reader, dst any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	dec.UseNumber()

	if err := dec.Decode(dst); err != nil {
		return err
	}

	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON payload")
		}
		return err
	}

	return nil
}

func UnmarshalStrictJSON(data []byte, dst any) error {
	return DecodeStrictJSON(strings.NewReader(string(data)), dst)
}
