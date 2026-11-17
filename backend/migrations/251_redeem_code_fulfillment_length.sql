-- Match the fixed-code admin fulfillment API, including durable activity reward codes.
ALTER TABLE redeem_codes ALTER COLUMN code TYPE VARCHAR(128);
