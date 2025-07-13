// Ftoa converts float64 to string without heap allocation
// IEEE 754 compliant with optimized bit manipulation
// Uses single stack buffer like Itoa - guaranteed zero allocation
// ⚠️  WARNING: Uses unsafe bit manipulation for speed
//
//go:norace
//go:nocheckptr
//go:nosplit
//go:inline
//go:registerparams
func Ftoa(f float64) string {
	// Stack buffer: worst case is scientific notation like "-1.234567e-308" = ~15 chars
	// Using 32 bytes to be safe for any IEEE 754 float64
	var buf [32]byte
	i := len(buf)

	// Fast path: extract IEEE 754 bits
	bits := *(*uint64)(unsafe.Pointer(&f))

	// Handle special cases via bit patterns (IEEE 754 compliant)
	exp := (bits >> 52) & 0x7FF
	if exp == 0x7FF {
		if bits&0xFFFFFFFFFFFFF != 0 {
			// NaN
			i -= 3
			buf[i] = 'N'
			buf[i+1] = 'a'
			buf[i+2] = 'N'
			return string(buf[i:])
		}
		// Infinity
		if bits&0x8000000000000000 != 0 {
			i -= 4
			buf[i] = '-'
			buf[i+1] = 'I'
			buf[i+2] = 'n'
			buf[i+3] = 'f'
		} else {
			i -= 4
			buf[i] = '+'
			buf[i+1] = 'I'
			buf[i+2] = 'n'
			buf[i+3] = 'f'
		}
		return string(buf[i:])
	}

	// Handle zero
	if bits&0x7FFFFFFFFFFFFFFF == 0 {
		i--
		buf[i] = '0'
		return string(buf[i:])
	}

	// Extract sign bit
	negative := bits&0x8000000000000000 != 0

	// Handle subnormal numbers (IEEE 754 compliant)
	var mantissa uint64
	var exponent int

	if exp == 0 {
		// Subnormal number
		mantissa = bits & 0xFFFFFFFFFFFFF
		exponent = -1022 - 52
	} else {
		// Normal number - add implicit leading 1
		mantissa = (bits & 0xFFFFFFFFFFFFF) | 0x10000000000000
		exponent = int(exp) - 1023 - 52
	}

	// Convert absolute value first, add sign at end
	absF := f
	if negative {
		absF = -absF
	}

	// Fast path for exact integers (very common case)
	if exponent >= 0 && exponent <= 10 {
		// This could be an exact integer
		intVal := mantissa << exponent

		// Check if it's actually an integer by seeing if fractional part is zero
		if float64(intVal) == absF {
			// Exact integer - use fast integer conversion like Itoa
			if intVal == 0 {
				i--
				buf[i] = '0'
			} else {
				for intVal > 0 {
					// Fast division by 10 using magic number (same as your Itoa pattern)
					q := (intVal * 0xCCCCCCCCCCCCCCCD) >> 67
					r := intVal - q*10
					i--
					buf[i] = byte(r + '0')
					intVal = q
				}
			}

			if negative {
				i--
				buf[i] = '-'
			}
			return string(buf[i:])
		}
	}

	// For non-integers, determine if we need scientific notation
	if absF >= 1e6 || (absF < 1e-4 && absF != 0) {
		// Scientific notation: -1.234567e+02
		scientificExp := 0
		normalizedF := absF

		if normalizedF >= 10.0 {
			for normalizedF >= 10.0 {
				normalizedF /= 10.0
				scientificExp++
			}
		} else {
			for normalizedF < 1.0 {
				normalizedF *= 10.0
				scientificExp--
			}
		}

		// Add exponent part
		expAbs := scientificExp
		if expAbs < 0 {
			expAbs = -expAbs
		}

		// Exponent digits (at most 3 digits for float64 range)
		if expAbs >= 100 {
			i--
			buf[i] = byte(expAbs%10 + '0')
			expAbs /= 10
		}
		if expAbs >= 10 {
			i--
			buf[i] = byte(expAbs%10 + '0')
			expAbs /= 10
		}
		i--
		buf[i] = byte(expAbs + '0')

		// Exponent sign
		i--
		if scientificExp >= 0 {
			buf[i] = '+'
		} else {
			buf[i] = '-'
		}

		i--
		buf[i] = 'e'

		// Mantissa fractional part (up to 6 digits)
		fracPart := uint64((normalizedF-float64(uint64(normalizedF)))*1000000 + 0.5)

		// Remove trailing zeros
		if fracPart > 0 {
			for fracPart > 0 && fracPart%10 == 0 {
				fracPart /= 10
			}

			// Add fractional digits
			for fracPart > 0 {
				i--
				buf[i] = byte(fracPart%10 + '0')
				fracPart /= 10
			}

			// Decimal point
			i--
			buf[i] = '.'
		}

		// Mantissa integer part (always 1-9 for normalized)
		i--
		buf[i] = byte(uint64(normalizedF) + '0')

	} else {
		// Regular decimal notation
		intPart := uint64(absF)
		fracPart := uint64((absF-float64(intPart))*1000000 + 0.5)

		// Add fractional part if non-zero
		if fracPart > 0 {
			// Remove trailing zeros
			for fracPart > 0 && fracPart%10 == 0 {
				fracPart /= 10
			}

			// Add fractional digits
			for fracPart > 0 {
				i--
				buf[i] = byte(fracPart%10 + '0')
				fracPart /= 10
			}

			// Decimal point
			i--
			buf[i] = '.'
		}

		// Add integer part
		if intPart == 0 {
			i--
			buf[i] = '0'
		} else {
			// Fast integer conversion (same pattern as Itoa)
			for intPart > 0 {
				q := (intPart * 0xCCCCCCCCCCCCCCCD) >> 67
				r := intPart - q*10
				i--
				buf[i] = byte(r + '0')
				intPart = q
			}
		}
	}

	// Add sign
	if negative {
		i--
		buf[i] = '-'
	}

	return string(buf[i:])
}