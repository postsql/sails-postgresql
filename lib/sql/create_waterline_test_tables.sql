
CREATE TABLE "document" (
    "title" TEXT PRIMARY KEY UNIQUE,
    "number" SERIAL,
    "serialNumber" TEXT  UNIQUE,
    "createdAt" TIMESTAMP WITH TIME ZONE,
    "updatedAt" TIMESTAMP WITH TIME ZONE
);

CREATE TABLE "pkfactory" (
    "number" INT,
    "createdAt" TIMESTAMP WITH TIME ZONE,
    "updatedAt" TIMESTAMP WITH TIME ZONE,
    "pkColumn" TEXT PRIMARY KEY UNIQUE
);

CREATE TABLE "user" (
    "first_name" TEXT,
    "last_name" TEXT,
    "email" TEXT,
    "title" TEXT,
    "phone" TEXT,
    "type" TEXT,
    "favoriteFruit" TEXT,
    "age" INT,
    "dob" TIMESTAMP WITH TIME ZONE,
    "status" BOOLEAN,
    "percent" FLOAT,
    "list" JSON,
    "obj" JSON,
    "id" SERIAL PRIMARY KEY UNIQUE,
    "createdAt" TIMESTAMP WITH TIME ZONE,
    "updatedAt" TIMESTAMP WITH TIME ZONE
);
