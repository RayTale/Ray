
CREATE TABLE IF Not EXISTS "public"."ray_tablelist" (
"prefix" varchar(255) COLLATE "default",
"name" varchar(255) COLLATE "default",
"version" int4,
"createtime" timestamp(6)
)
WITH (OIDS=FALSE);
CREATE UNIQUE INDEX "table_version" ON "public"."ray_tablelist" USING btree ("prefix", "version");
