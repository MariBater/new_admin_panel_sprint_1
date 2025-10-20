CREATE SCHEMA IF NOT EXISTS content;


CREATE TABLE IF NOT EXISTS content.film_work (
    id UUID PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE,
    modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.person (
    id UUID PRIMARY KEY,
    full_name TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE,
    modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id UUID PRIMARY KEY,
    person_id UUID NOT NULL,
    film_work_id UUID NOT NULL,
    role TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE,
    CONSTRAINT fk_pfw_person_id
        FOREIGN KEY (person_id)
        REFERENCES content.person (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_pfw_film_work_id
        FOREIGN KEY (film_work_id)
        REFERENCES content.film_work (id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS content.genre (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created TIMESTAMP WITH TIME ZONE,
    modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id UUID PRIMARY KEY,
    genre_id UUID NOT NULL,
    film_work_id UUID NOT NULL,
    created TIMESTAMP WITH TIME ZONE,
    CONSTRAINT fk_gfw_genre_id
        FOREIGN KEY (genre_id)
        REFERENCES content.genre (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_gfw_film_work_id
        FOREIGN KEY (film_work_id)
        REFERENCES content.film_work (id)
        ON DELETE CASCADE
);


CREATE INDEX IF NOT EXISTS film_work_creation_rating_idx ON content.film_work (creation_date, rating);

CREATE UNIQUE INDEX IF NOT EXISTS film_work_person_role_idx ON content.person_film_work (film_work_id, person_id, role);

CREATE UNIQUE INDEX IF NOT EXISTS film_work_genre_idx ON content.genre_film_work (film_work_id, genre_id);
