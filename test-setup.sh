#!/bin/bash
set -e

echo "Testing environment setup..."
echo ""

echo "Java:"
java -version
echo ""

echo "Maven:"
mvn -version
echo ""

echo "Docker:"
docker --version
docker compose version
echo ""

echo "✅ All tools are working!"
echo ""
echo "Next steps:"
echo "  mvn clean package"
echo "  docker compose up -d zookeeper kafka kafka-ui redis"
echo "  mvn spring-boot:run -Dspring-boot.run.profiles=production"
