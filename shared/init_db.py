"""Creates the orders table if it doesn't exist. Run once before the
producer/consumer start (docker-compose does this via an init step;
tests call it directly against the test database)."""
from shared.models import Base, get_engine

if __name__ == "__main__":
    Base.metadata.create_all(get_engine())
    print("orders table ready")
