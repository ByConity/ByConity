select *;

-- TODO: qualified asterisk is not supported by optimizer yet
SET enable_optimizer = 0;
--error: should be failed for abc.*;
select abc.*; --{serverError 47}
select *, abc.*; --{serverError 47}
select abc.*, *; --{serverError 47}
