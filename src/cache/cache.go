package cache

type StateCache struct {
	DeviceID          string
	LastEvent         string
	LastTimestampSeen int
}
