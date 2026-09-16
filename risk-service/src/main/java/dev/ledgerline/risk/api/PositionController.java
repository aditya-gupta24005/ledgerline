package dev.ledgerline.risk.api;

import dev.ledgerline.risk.query.PositionQueryService;
import dev.ledgerline.risk.query.PositionView;
import dev.ledgerline.risk.query.RiskDataUnavailableException;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/risk/accounts")
class PositionController {

    private final PositionQueryService queryService;

    PositionController(PositionQueryService queryService) {
        this.queryService = queryService;
    }

    /** Risk and ops see every account; a trader only their own (the token's username). */
    @GetMapping("/{accountId}/positions")
    @PreAuthorize("hasAnyRole('RISK', 'OPS') or #accountId == authentication.name")
    public List<PositionView> positions(@PathVariable String accountId) {
        return queryService.positionsFor(accountId);
    }

    @ExceptionHandler(RiskDataUnavailableException.class)
    ProblemDetail handleUnavailable(RiskDataUnavailableException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
        problem.setTitle("Risk data unavailable");
        return problem;
    }
}
