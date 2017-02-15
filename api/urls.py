from django.conf.urls import patterns, url
from api import views

urlpatterns = patterns('',
    url(r'^data$', views.data),
    url(r'^zabbixdata$', views.zabbixdata),
    url(r'^zabbixapp$', views.zabbixapp),
    url(r'^analysis$', views.analysis),
    url(r'^add_tree$', views.add_tree),
    url(r'^get_tree$', views.get_tree),
    url(r'^delete_tree$', views.delete_tree),
    url(r'^event$', views.event),
    url(r'^top$', views.top),
    url(r'^kafka$', views.kafka),
)
