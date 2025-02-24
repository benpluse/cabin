from .models import *
from django.db.models import Sum, Count, Max, Min, Q, ExpressionWrapper, F, DurationField


def query_0():
    q = Driver.objects.all()
    return q


def query_1():
    total_amount = Payment.objects.aggregate(total=Sum('amount'))['total']
    return total_amount


def query_2(x):
    try:
        rider = Rider.objects.get(id=x)
        total_amount = Payment.objects.filter(ride__request__rider=rider).aggregate(total=Sum('amount'))['total'] or 0
        print(f'total amount paid by user{x}:{total_amount}')
        return total_amount
    except Rider.DoesNotExist:
        print('Account does not exist')



def query_3():
    a_driver = Driver.objects.filter(car__car_type='A').annotate(total=Count('id'))
    return a_driver.count()


def query_4():
    not_connected = RideRequest.objects.filter(ride__isnull=True)
    return list(not_connected)


def query_5(t):
    riders = Rider.objects.aggregate(total_payment=Sum('riderequest__ride__payment__amount').filter(total_payment__gte=t))
    return list(riders)


def query_6():
    max_car_count = Account.objects.annotate(total=Count('drivers__car')).aggregate(Max('total'))['total__max']
    accounts_with_max_cars = Account.objects.annotate(total=Count('drivers__car')).filter(total=max_car_count)
    min_last_name = accounts_with_max_cars.aggregate(min_ln=Min('last_name'))['min_ln']
    accounts_with_min_last_name = accounts_with_max_cars.filter(last_name=min_last_name)

    if accounts_with_max_cars.count() < 2:
        return list(accounts_with_max_cars)
    else:
        return accounts_with_min_last_name


def query_7():
    n = Rider.objects.filter(riderequest__car_type='A').annotate(total=Count('id'))
    return n.count()


def query_8(x):
    driver = Account.objects.filter(drivers__car__model=x)
    return driver.values('email')


def query_9():
    rides = Driver.objects.annotate(n=Count('car__ride')).values('account', 'n')
    return rides


def query_10():
    rides = Account.objects.annotate(n=Count('drivers__car__ride')).values('first_name', 'n').distinct()
    return rides


def query_11(n, c):
    drivers = Driver.objects.filter(car__model__gte=n, car__color__iexact=c).distinct()
    return drivers



def query_12(n, c):
    drivers = Driver.objects.filter(Q(car__model__gte=n) | Q(car__color__iexact=c)).distinct()
    return  drivers


def query_13(n, m):
    ride_duration = Ride.objects.filter(Q(car__owner__account__first_name=n) & Q(riderequest__rider__account__first_name=m)).annotate(
        ride_duration=ExpressionWrapper(F('dropoff_time') - F('pickup_time'), output_field=DurationField())
    ).aggregate(total_duration=Sum('ride_duration'))

    return ride_duration