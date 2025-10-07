FROM nginx:1.25-alpine

# Копируем кастомную конфигурацию Nginx
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Создаем директории для статики и медиа
RUN mkdir -p /var/www/static /var/www/media
