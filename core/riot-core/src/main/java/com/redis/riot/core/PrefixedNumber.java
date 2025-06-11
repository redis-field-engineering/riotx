package com.redis.riot.core;

import org.springframework.util.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A numeric quantity with a prefix, such as '10K'. This class models numeric values with standard prefixes and is immutable and
 * thread-safe.
 *
 * <p>The terms and units used in this class are based on
 * standard metric prefixes indicating multiplication by powers of 10. Consult the following table for details.
 *
 * <p>
 * <table border="1">
 * <tr><th>Term</th><th>Prefix</th><th>Multiplier</th></tr>
 * <tr><td>kilo</td><td>K</td><td>1,000</td></tr>
 * <tr><td>mega</td><td>M</td><td>1,000,000</td></tr>
 * <tr><td>giga</td><td>G</td><td>1,000,000,000</td></tr>
 * </table>
 */
public final class PrefixedNumber implements Comparable<PrefixedNumber> {

    /**
     * Base multiplier for kilo.
     */
    private static final long KILO_MULTIPLIER = 1_000L;

    /**
     * Base multiplier for mega.
     */
    private static final long MEGA_MULTIPLIER = 1_000_000L;

    /**
     * Base multiplier for giga.
     */
    private static final long GIGA_MULTIPLIER = 1_000_000_000L;

    /**
     * The pattern for parsing.
     */
    private static final Pattern PATTERN = Pattern.compile("^([+\\-]?\\d+)([kKmMgG]?)$");

    private long value;

    public PrefixedNumber() {
    }

    private PrefixedNumber(long value) {
        this.value = value;
    }

    /**
     * Obtain a {@link PrefixedNumber} representing the specified number.
     *
     * @param value the number value, positive or negative
     * @return a {@code PrefixedNumber}
     */
    public static PrefixedNumber of(long value) {
        return new PrefixedNumber(value);
    }

    /**
     * Obtain a {@link PrefixedNumber} representing the specified number of kilos.
     *
     * @param kilos the number of kilos, positive or negative
     * @return a {@code PrefixedNumber}
     */
    public static PrefixedNumber ofKilo(long kilos) {
        return new PrefixedNumber(Math.multiplyExact(kilos, KILO_MULTIPLIER));
    }

    /**
     * Obtain a {@link PrefixedNumber} representing the specified number of megas.
     *
     * @param megas the number of megas, positive or negative
     * @return a {@code PrefixedNumber}
     */
    public static PrefixedNumber ofMega(long megas) {
        return new PrefixedNumber(Math.multiplyExact(megas, MEGA_MULTIPLIER));
    }

    /**
     * Obtain a {@link PrefixedNumber} representing the specified number of gigas.
     *
     * @param gigas the number of gigas, positive or negative
     * @return a {@code PrefixedNumber}
     */
    public static PrefixedNumber ofGiga(long gigas) {
        return new PrefixedNumber(Math.multiplyExact(gigas, GIGA_MULTIPLIER));
    }

    /**
     * Obtain a {@link PrefixedNumber} representing an amount in the specified {@link NumberPrefix}.
     *
     * @param amount the amount, measured in terms of the prefix unit, positive or negative
     * @param prefix the prefix to use
     * @return a corresponding {@code PrefixedNumber}
     */
    public static PrefixedNumber of(long amount, NumberPrefix prefix) {
        if (prefix == null) {
            throw new IllegalArgumentException("Prefix must not be null");
        }
        return new PrefixedNumber(Math.multiplyExact(amount, prefix.getMultiplier()));
    }

    /**
     * Obtain a {@link PrefixedNumber} from a text string such as {@code 12K}. If no unit is specified, the value is interpreted
     * as a raw number.
     * <p>Examples:
     * <pre>
     * "12K" -- parses as "12 kilo"
     * "5M"  -- parses as "5 mega"
     * "20"  -- parses as "20"
     * </pre>
     *
     * @param text the text to parse
     * @return the parsed {@code PrefixedNumber}
     */
    public static PrefixedNumber parse(CharSequence text) {
        return parse(text, null);
    }

    /**
     * Obtain a {@link PrefixedNumber} from a text string such as {@code 12K}, using the specified default {@link NumberPrefix}
     * if no prefix is specified.
     * <p>Examples:
     * <pre>
     * "12K" -- parses as "12 kilo"
     * "5M"  -- parses as "5 mega"
     * "20"  -- parses as "20 kilo" (where the defaultPrefix is NumberPrefix.KILO)
     * "20"  -- parses as "20" (if the defaultPrefix is null)
     * </pre>
     *
     * @param text the text to parse
     * @param defaultPrefix the default prefix to use if none is specified
     * @return the parsed {@code PrefixedNumber}
     */
    public static PrefixedNumber parse(CharSequence text, NumberPrefix defaultPrefix) {
        if (!StringUtils.hasText(text)) {
            return null;
        }
        String trimmedText = text.toString().trim();
        try {
            Matcher matcher = PATTERN.matcher(trimmedText);

            if (!matcher.matches()) {
                throw new IllegalArgumentException("'" + text + "' does not match prefixed number pattern");
            }

            String value = matcher.group(1);
            String suffix = matcher.group(2);

            long amount = Long.parseLong(value);
            NumberPrefix prefix = determinePrefix(suffix, defaultPrefix);

            if (prefix == null) {
                return PrefixedNumber.of(amount);
            } else {
                return PrefixedNumber.of(amount, prefix);
            }
        } catch (Exception ex) {
            throw new IllegalArgumentException("'" + text + "' is not a valid prefixed number", ex);
        }
    }

    private static NumberPrefix determinePrefix(String suffix, NumberPrefix defaultPrefix) {
        if (suffix == null || suffix.isEmpty()) {
            return defaultPrefix;
        }

        char c = suffix.toLowerCase().charAt(0);
        return switch (c) {
            case 'k' -> NumberPrefix.KILO;
            case 'm' -> NumberPrefix.MEGA;
            case 'g' -> NumberPrefix.GIGA;
            default -> throw new IllegalArgumentException("Unknown prefix suffix: " + suffix);
        };
    }

    /**
     * Checks if this value is negative, excluding zero.
     *
     * @return true if this value is less than zero
     */
    public boolean isNegative() {
        return this.value < 0;
    }

    /**
     * Return the raw number value.
     *
     * @return the raw number value
     */
    public long getValue() {
        return this.value;
    }

    /**
     * Return the value in kilo units.
     *
     * @return the number of kilos
     */
    public long toKilo() {
        return this.value / KILO_MULTIPLIER;
    }

    /**
     * Return the value in mega units.
     *
     * @return the number of megas
     */
    public long toMega() {
        return this.value / MEGA_MULTIPLIER;
    }

    /**
     * Return the value in giga units.
     *
     * @return the number of gigas
     */
    public long toGiga() {
        return this.value / GIGA_MULTIPLIER;
    }

    @Override
    public int compareTo(PrefixedNumber other) {
        return Long.compare(this.value, other.value);
    }

    @Override
    public String toString() {
        if (this.value % GIGA_MULTIPLIER == 0) {
            return this.value / GIGA_MULTIPLIER + "G";
        } else if (this.value % MEGA_MULTIPLIER == 0) {
            return this.value / MEGA_MULTIPLIER + "M";
        } else if (this.value % KILO_MULTIPLIER == 0) {
            return this.value / KILO_MULTIPLIER + "K";
        } else {
            return Long.toString(this.value);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        PrefixedNumber that = (PrefixedNumber) other;
        return (this.value == that.value);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.value);
    }

    public int intValue() {
        return Math.toIntExact(getValue());
    }

    /**
     * Enumeration of common number prefixes.
     */
    public enum NumberPrefix {

        /**
         * Kilo (K) unit, representing 1,000.
         */
        KILO(KILO_MULTIPLIER, "K"),

        /**
         * Mega (M) unit, representing 1,000,000.
         */
        MEGA(MEGA_MULTIPLIER, "M"),

        /**
         * Giga (G) unit, representing 1,000,000,000.
         */
        GIGA(GIGA_MULTIPLIER, "G");

        private final long multiplier;

        private final String suffix;

        NumberPrefix(long multiplier, String suffix) {
            this.multiplier = multiplier;
            this.suffix = suffix;
        }

        /**
         * Returns the multiplier associated with this prefix.
         *
         * @return the multiplier
         */
        public long getMultiplier() {
            return this.multiplier;
        }

        /**
         * Returns the suffix used for this prefix.
         *
         * @return the suffix
         */
        public String getSuffix() {
            return this.suffix;
        }

        /**
         * Get a {@link PrefixedNumber} instance representing the given amount in this prefix's unit.
         *
         * @param amount the amount
         * @return a {@code PrefixedNumber} instance
         */
        public PrefixedNumber of(long amount) {
            return PrefixedNumber.of(amount, this);
        }
    }

}
