# Stage 1 - the build process
FROM node:18-slim as build
WORKDIR /app
COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile
COPY . ./
RUN yarn build

# Stage 2 - the production environment
FROM nginx:stable-alpine
COPY --from=build /app/build /usr/share/nginx/html
# Corrected path
COPY ./nginx/default.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]