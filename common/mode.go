package common

const (
	// DebugMode indicates mode is debug.
	DebugMode = "debug"
	// ReleaseMode indicates mode is release.
	ReleaseMode = "release"
)

var modeName = DebugMode

// SetMode sets  mode according to input string.
func SetMode(value string) {
	if value == "" {
		value = DebugMode
	}
	modeName = value
}

// Mode returns currently mode.
func Mode() string {
	return modeName
}
