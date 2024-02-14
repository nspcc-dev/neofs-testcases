package testdata

func _deploy(data interface{}, isUpdate bool) {
	if isUpdate && data.(string) == "shouldFail" {
		panic("update has failed")
	}
}

// GetThree is a simple function which does nothing.
func GetThree() int {
	return 42
}
