COMMON_OUT=../libs/common/src
SERVER_OUT=../libs/server/src

PBS=$(COMMON_OUT)/feder/common/points_pb2.py $(SERVER_OUT)/feder/server/rabbitmq_pb2.py

.PHONY: all clean
all: $(PBS)

clean:
	$(RM) $(PBS)

$(COMMON_OUT)/feder/common/points_pb2.py: feder/common/points.proto
	protoc --proto_path=. --python_out=$(COMMON_OUT) $^

$(SERVER_OUT)/feder/server/rabbitmq_pb2.py: feder/server/rabbitmq.proto
	protoc --proto_path=. --python_out=$(SERVER_OUT) $^
