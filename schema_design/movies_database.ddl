CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Создайте схему
CREATE SCHEMA IF NOT EXISTS content;

-- Жанры
CREATE TABLE content.genre (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Кинопроизведения
CREATE TABLE content.film_work (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(255) NOT NULL,
    description TEXT,
    creation_date DATE,
    rating NUMERIC(3,1) CHECK (rating >= 0 AND rating <= 10),
    type VARCHAR(20) NOT NULL,
    created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Участники
CREATE TABLE content.person (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name VARCHAR(255) NOT NULL,
    created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Связь: Жанры  Кинопроизведения
CREATE TABLE content.genre_film_work (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    genre_id UUID NOT NULL,
    film_work_id UUID NOT NULL,
    created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (genre_id, film_work_id),
    
    FOREIGN KEY (genre_id) 
        REFERENCES content.genre(id) 
        ON DELETE CASCADE,
        
    FOREIGN KEY (film_work_id) 
        REFERENCES content.film_work(id) 
        ON DELETE CASCADE
);

-- Связь: Участники Кинопроизведения + Роль
CREATE TABLE content.person_film_work (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    person_id UUID NOT NULL,
    film_work_id UUID NOT NULL,
    role VARCHAR(50) NOT NULL,
    created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (person_id, film_work_id, role),
    
    FOREIGN KEY (person_id) 
        REFERENCES content.person(id) 
        ON DELETE CASCADE,
        
    FOREIGN KEY (film_work_id) 
        REFERENCES content.film_work(id) 
        ON DELETE CASCADE
);

-- Индексы (аналогичны предыдущим)
CREATE INDEX idx_film_work_title ON content.film_work(title);
CREATE INDEX idx_film_work_rating ON content.film_work(rating);
CREATE INDEX idx_film_work_creation_date ON content.film_work(creation_date);
CREATE INDEX idx_person_full_name ON content.person(full_name);
CREATE INDEX idx_genre_film_work_film ON content.genre_film_work(film_work_id);
CREATE INDEX idx_person_film_work_film ON content.person_film_work(film_work_id);
CREATE INDEX idx_person_film_work_person ON content.person_film_work(person_id);
CREATE INDEX idx_person_film_work_role ON content.person_film_work(role);