CREATE TABLE settlement_instructions (
    trade_id         VARCHAR(64)   PRIMARY KEY,
    symbol           VARCHAR(16)   NOT NULL,
    buy_account_id   VARCHAR(64)   NOT NULL,
    sell_account_id  VARCHAR(64)   NOT NULL,
    price            NUMERIC(19,4) NOT NULL CHECK (price > 0),
    quantity         BIGINT        NOT NULL CHECK (quantity > 0),
    currency         VARCHAR(3)    NOT NULL,
    trade_date       DATE          NOT NULL,
    settlement_date  DATE          NOT NULL,
    status           VARCHAR(16)   NOT NULL,
    created_at       TIMESTAMPTZ   NOT NULL
);

CREATE INDEX idx_settlement_instructions_status_date
    ON settlement_instructions (status, settlement_date);

CREATE TABLE journal_entries (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trade_id    VARCHAR(64)   NOT NULL REFERENCES settlement_instructions (trade_id),
    account_id  VARCHAR(64)   NOT NULL,
    asset       VARCHAR(16)   NOT NULL,
    amount      NUMERIC(24,4) NOT NULL,
    posted_at   TIMESTAMPTZ   NOT NULL
);

CREATE INDEX idx_journal_entries_account_asset ON journal_entries (account_id, asset);
CREATE INDEX idx_journal_entries_trade ON journal_entries (trade_id);
