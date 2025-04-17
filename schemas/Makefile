SERVER_OUT=../libs/server/src

PBS=\
  $(SERVER_OUT)/feder/server/liveness_pb2.py \
  $(SERVER_OUT)/feder/server/trajectory_pb2.py

.PHONY: all clean
all: $(PBS)

clean:
	$(RM) $(PBS)

$(SERVER_OUT)/feder/server/liveness_pb2.py: feder/server/liveness.proto
	protoc --proto_path=. --python_out=$(SERVER_OUT) $^

$(SERVER_OUT)/feder/server/trajectory_pb2.py: feder/server/trajectory.proto
	protoc --proto_path=. --python_out=$(SERVER_OUT) $^
