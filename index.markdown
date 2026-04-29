---
layout: default
title: "About Me"
---

Hi, I'm **YuzhengWu** — an AWS-based cloud and data engineer. I created this blog to document the real technical challenges I run into at work and the solutions I've found.

My work is focused on the data domain. On the AWS side, I regularly work with services including **Connect, Kinesis, Firehose, S3, Glue, Athena, Lake Formation, IAM, and RAM**. Beyond AWS, I also work with open-source tools across the modern data stack: **Kafka, Airflow, PySpark, Cassandra, Docker, Prometheus, and Grafana**.

## Posts

{% for post in site.posts %}
- [{{ post.title }}]({{ post.url | prepend: site.baseurl }}) — {{ post.date | date: "%b %d, %Y" }}
{% endfor %}
