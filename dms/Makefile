.PHONY: test
test:
	@docker-compose -f tests/docker-compose.yml up -d
	@cargo test -- --test-threads=1 --show-output
	@docker-compose -f tests/docker-compose.yml down