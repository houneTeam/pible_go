package bluetooth

import (
	tg "tinygo.org/x/bluetooth"
)

// ClassifyAddress returns the address type and (when random) the random subtype.
//
// Type:
//   - "public_or_unknown": stack did not mark address as random
//   - "random": random device address
//
// Subtype (only when type=="random") based on the two MSBs of the address:
//   - 00: non_resolvable_private
//   - 01: resolvable_private
//   - 10: reserved
//   - 11: static_random
func ClassifyAddress(addr tg.Address) (typ string, sub string) {
	if !addr.IsRandom() {
		return "public_or_unknown", ""
	}
	b, err := addr.MAC.MarshalBinary()
	if err != nil || len(b) < 1 {
		return "random", ""
	}
	msb2 := (b[0] >> 6) & 0x03
	switch msb2 {
	case 0:
		return "random", "non_resolvable_private"
	case 1:
		return "random", "resolvable_private"
	case 2:
		return "random", "reserved"
	case 3:
		return "random", "static_random"
	default:
		return "random", ""
	}
}
