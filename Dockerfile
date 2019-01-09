FROM golang:1.10 as builder
WORKDIR /go/src/k8s.io/add-ebs-tags-controller
ADD ./  /go/src/k8s.io/add-ebs-tags-controller
RUN CGO_ENABLED=0 go build


FROM alpine
RUN apk add -U tzdata
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai  /etc/localtime
COPY --from=builder /go/src/k8s.io/add-ebs-tags-controller/add-ebs-tags-controller /usr/local/bin/add-ebs-tags-controller
RUN chmod +x /usr/local/bin/add-ebs-tags-controller
CMD ["add-ebs-tags-controller"]