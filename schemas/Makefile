PYTHON_OUT=../libs/common/feder/common

$(PYTHON_OUT)/points_pb2.py: points.proto
	protoc --proto_path=. --python_out=$(PYTHON_OUT) points.proto
