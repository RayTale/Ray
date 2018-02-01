
CREATE TABLE "public"."ex_account_event_1" (
"id" varchar(30) COLLATE "default" NOT NULL,
"stateid" varchar(30) COLLATE "default" NOT NULL,
"msgid" varchar(30) COLLATE "default" NOT NULL,
"typecode" varchar(100) COLLATE "default" NOT NULL,
"iscomplete" bool NOT NULL,
"data" bytea NOT NULL,
"version" int8 NOT NULL
)
WITH (OIDS=FALSE)£»

CREATE UNIQUE INDEX "State_MsgId" ON "public"."ex_account_event_1" USING btree ("stateid", "msgid", "typecode");
CREATE UNIQUE INDEX "State_Version" ON "public"."ex_account_event_1" USING btree ("stateid", "version");

ALTER TABLE "public"."ex_account_event_1" ADD PRIMARY KEY ("id");
