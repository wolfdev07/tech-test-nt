Cargo
id VARCHAR(24) NOT NULL
company_name VARCHAR(130) NULL
company_id VARCHAR(24) NOT NULL
amount DECIMAL(16,2) NOT NULL
status VARCHAR(30) NOT NULL
created_at TIMESTAMP NOT NULL
updated_at TIMESTAMP NULL

CREATE TABLE companies if NOT EXISTS (
    id VARCHAR(24) PRIMARY KEY,
    name VARCHAR(130) NOT NULL
);

CREATE TABLE charges if NOT EXISTS (
    id VARCHAR(24) PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    amount DECIMAL(16,2) NOT NULL,
    status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NULL
);
