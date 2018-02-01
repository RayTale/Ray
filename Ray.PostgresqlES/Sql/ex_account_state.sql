
CREATE TABLE "public"."ex_account_state" (
"stateid" varchar(30) COLLATE "default" NOT NULL,
"data" bytea NOT NULL
)
WITH (OIDS=FALSE);
ALTER TABLE "public"."ex_account_state" ADD PRIMARY KEY ("stateid");
