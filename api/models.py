from django.db import models

class Dependent(models.Model):
    _type = (
        ('netscaler', 'netscaler'),
        ('extranet_nginx', 'extranet_nginx'),
        ('mainApp', 'mainApp'),
        ('jumpApp', 'jumpApp'),
        ('haproxy', 'haproxy'),
        ('app', 'app')
    )
    domain = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    type = models.CharField(max_length=50, choices=_type)
    creator = models.CharField(max_length=50, null=True, blank=True, default=None)
    last_modified_by = models.CharField(max_length=50, null=True, blank=True, default=None)
    created_date = models.DateTimeField(auto_now_add=True)
    modified_date = models.DateTimeField(auto_now=True)
    pid = models.ForeignKey('self', null=True, blank=True, default=None)

    def __unicode__(self):
        return self.name

class Forward(models.Model):
    domain = models.CharField(max_length=25)
    path = models.CharField(max_length=25)
    app = models.CharField(max_length=25)

    def __unicode__(self):
        return self.domain
