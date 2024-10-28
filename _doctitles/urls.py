from django.urls import include, path
from rest_framework import routers
from _doctitles import views

# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    # path('', include(router.urls)),
    # path(r'', views.test.as_view()),
    path(r'', views.Doctitles.as_view(), name='doctitles'),
]
