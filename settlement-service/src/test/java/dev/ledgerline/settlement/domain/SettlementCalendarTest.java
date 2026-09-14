package dev.ledgerline.settlement.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SettlementCalendarTest {

    @ParameterizedTest(name = "{0} + {1} business days = {2}")
    @CsvSource({
            "2026-09-14, 1, 2026-09-15", // Monday -> Tuesday
            "2026-09-18, 1, 2026-09-21", // Friday -> Monday
            "2026-09-19, 1, 2026-09-21", // Saturday -> Monday
            "2026-09-17, 2, 2026-09-21", // Thursday T+2 -> Monday
            "2026-09-14, 0, 2026-09-14"
    })
    void skipsWeekends(LocalDate tradeDate, int businessDays, LocalDate expected) {
        assertThat(SettlementCalendar.addBusinessDays(tradeDate, businessDays)).isEqualTo(expected);
    }
}
