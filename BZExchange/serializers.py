from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework_simplejwt.views import TokenObtainPairView
from datetime import timedelta, datetime


class MyTokenObtainPairSerializer(TokenObtainPairSerializer):
    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)
        token['name'] = str(user)
        return token


class MyTokenObtainPairView(TokenObtainPairView):
    serializer_class = MyTokenObtainPairSerializer

    def post(self, request, *args, **kwargs):
        response = super().post(request, *args, **kwargs)
        # Customize response data here
        token = response.data.get('access')  # Assuming using Access token
        expires = datetime.utcnow() + timedelta(minutes=15)
        if token:
            response.data['access_token'] = token
            response.data['expires'] = expires.strftime("%Y-%m-%dT%H:%M:%SZ")
            del response.data['access']
            del response.data['refresh']
        return response