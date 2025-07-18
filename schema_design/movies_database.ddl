-- Создаем схему, если она еще не существует
CREATE SCHEMA IF NOT EXISTS content;

-- Устанавливаем pgcrypto для генерации UUID
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Функция для обновления поля modified
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Таблица для жанров
CREATE TABLE IF NOT EXISTS content.genre (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created TIMESTAMP WITH TIME ZONE DEFAULT now(),
    modified TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Триггер для genre
CREATE TRIGGER update_genre_modtime
BEFORE UPDATE ON content.genre
FOR EACH ROW
EXECUTE PROCEDURE update_modified_column();

-- Таблица для персон
CREATE TABLE IF NOT EXISTS content.person (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name VARCHAR(255) NOT NULL,
    created TIMESTAMP WITH TIME ZONE DEFAULT now(),
    modified TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Триггер для person
CREATE TRIGGER update_person_modtime
BEFORE UPDATE ON content.person
FOR EACH ROW
EXECUTE PROCEDURE update_modified_column();

-- Таблица для кинопроизведений
CREATE TABLE IF NOT EXISTS content.film_work (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(255) NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT CHECK (rating >= 0 AND rating <= 10),
    type VARCHAR(50) NOT NULL,
    created TIMESTAMP WITH TIME ZONE DEFAULT now(),
    modified TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Триггер для film_work
CREATE TRIGGER update_film_work_modtime
BEFORE UPDATE ON content.film_work
FOR EACH ROW
EXECUTE PROCEDURE update_modified_column();

-- Таблица для связи кинопроизведений и жанров (многие-ко-многим)
CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    film_work_id UUID NOT NULL REFERENCES content.film_work(id) ON DELETE CASCADE,
    genre_id UUID NOT NULL REFERENCES content.genre(id) ON DELETE CASCADE,
    created TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT film_work_genre_unique UNIQUE (film_work_id, genre_id)
);

-- Таблица для связи кинопроизведений и персон (многие-ко-многим)
CREATE TABLE IF NOT EXISTS content.person_film_work (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    film_work_id UUID NOT NULL REFERENCES content.film_work(id) ON DELETE CASCADE,
    person_id UUID NOT NULL REFERENCES content.person(id) ON DELETE CASCADE,
    role VARCHAR(255) NOT NULL,
    created TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT film_work_person_role_unique UNIQUE (film_work_id, person_id, role)
);

-- Индексы для ускорения поиска по внешним ключам
CREATE INDEX IF NOT EXISTS genre_film_work_film_work_id_idx ON content.genre_film_work (film_work_id);
CREATE INDEX IF NOT EXISTS genre_film_work_genre_id_idx ON content.genre_film_work (genre_id);
CREATE INDEX IF NOT EXISTS person_film_work_film_work_id_idx ON content.person_film_work (film_work_id);
CREATE INDEX IF NOT EXISTS person_film_work_person_id_idx ON content.person_film_work (person_id);

-- Индекс для поиска по дате создания кинопроизведения
CREATE INDEX IF NOT EXISTS film_work_creation_date_idx ON content.film_work (creation_date);