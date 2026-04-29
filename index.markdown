---
layout: default
title: "About Me"
---

Passionate about data engineering and cloud infrastructure.
Currently working with AWS Connect, Kinesis, S3, Glue, and Athena.

## Posts

{% for post in site.posts %}
- [{{ post.title }}]({{ post.url | prepend: site.baseurl }}) — {{ post.date | date: "%b %d, %Y" }}
{% endfor %}
