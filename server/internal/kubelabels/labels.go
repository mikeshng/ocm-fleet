// Package kubelabels validates label keys and values against the
// Kubernetes label syntax. Labels accepted from a caller become labels
// on Kubernetes objects, target labels, or label selectors, so every
// producer and consumer checks them the same way.
package kubelabels

import (
	"k8s.io/apimachinery/pkg/api/validate/content"
)

// ValidKey reports whether key is a valid Kubernetes label key: an
// optional DNS subdomain prefix and a '/', then a name of at most 63
// characters from [A-Za-z0-9_.-] that starts and ends with an
// alphanumeric character.
func ValidKey(key string) bool {
	return len(content.IsLabelKey(key)) == 0
}

// ValidValue reports whether value is a valid Kubernetes label value:
// empty, or at most 63 characters from [A-Za-z0-9_.-] that start and
// end with an alphanumeric character.
func ValidValue(value string) bool {
	return len(content.IsLabelValue(value)) == 0
}

// Valid reports whether both the key and the value are valid.
func Valid(key, value string) bool {
	return ValidKey(key) && ValidValue(value)
}
