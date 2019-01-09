FROM golang:1.10 as builder
WORKDIR /go/src/k8s.io/add-ebs-tags-controller
ADD ./  /go/src/k8s.io/add-ebs-tags-controller
RUN CGO_ENABLED=0 go build


FROM geekidea/alpine-a:3.8
COPY --from=builder /go/src/k8s.io/add-ebs-tags-controller/add-ebs-tags-controller /usr/local/bin/add-ebs-tags-controller
RUN chmod +x /usr/local/bin/add-ebs-tags-controller
CMD ["add-ebs-tags-controller"]