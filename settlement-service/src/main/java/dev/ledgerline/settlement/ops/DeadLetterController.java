package dev.ledgerline.settlement.ops;

import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/ops/dead-letters")
class DeadLetterController {

    private static final int MAX_LIMIT = 500;

    private final DeadLetterStore store;
    private final DeadLetterReplayer replayer;

    DeadLetterController(DeadLetterStore store, DeadLetterReplayer replayer) {
        this.store = store;
        this.replayer = replayer;
    }

    @GetMapping
    List<DeadLetterRecord> list(@RequestParam(defaultValue = "50") int limit) {
        return store.latest(Math.clamp(limit, 1, MAX_LIMIT));
    }

    @PostMapping("/{partition}/{offset}/replay")
    ResponseEntity<ReplayResult> replay(@PathVariable int partition, @PathVariable long offset) {
        return replayer.replay(partition, offset)
                .map(result -> ResponseEntity.accepted().body(result))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @ExceptionHandler(UnknownDeadLetterPartitionException.class)
    ProblemDetail handleUnknownPartition(UnknownDeadLetterPartitionException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
        problem.setTitle("Unknown dead-letter partition");
        return problem;
    }
}
