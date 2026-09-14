package dev.ledgerline.settlement.domain;

import java.time.DayOfWeek;
import java.time.LocalDate;

/** Business-day arithmetic for settlement dates. Weekends only for now; exchange holidays are on the roadmap. */
public final class SettlementCalendar {

    private SettlementCalendar() {
    }

    public static LocalDate addBusinessDays(LocalDate date, int businessDays) {
        if (businessDays < 0) {
            throw new IllegalArgumentException("businessDays must not be negative");
        }
        LocalDate result = date;
        int remaining = businessDays;
        while (remaining > 0) {
            result = result.plusDays(1);
            if (!isWeekend(result)) {
                remaining--;
            }
        }
        return result;
    }

    private static boolean isWeekend(LocalDate date) {
        DayOfWeek day = date.getDayOfWeek();
        return day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY;
    }
}
