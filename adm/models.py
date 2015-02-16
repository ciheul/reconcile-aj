from django.db import models
from django.conf import settings
from django.contrib.auth.models import (Group, AbstractBaseUser,
                                        PermissionsMixin, UserManager,
                                        BaseUserManager)
from mptt.models import MPTTModel, TreeForeignKey
from django.core import validators
from django.core.urlresolvers import reverse
from django.utils.translation import ugettext_lazy as _
from django.utils import timezone

from actstream import registry

import re
from redis import Redis

r = Redis('localhost')


class Account(MPTTModel):
    name = models.CharField(max_length=50, unique=True)
    parent = TreeForeignKey('self', null=True,
                            blank=True, related_name="child")
    ip_address = models.GenericIPAddressField(_('IP Address'), null=True,
                                              blank=True)
    is_active = models.BooleanField(default=True)
    date_joined = models.DateTimeField(auto_now_add=True, auto_now=False)
    balance = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    fee = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    username = models.CharField(_('User'), max_length=20,
                                unique=True, null=True, blank=True)
    pin = models.CharField(max_length=20, unique=True, null=True, blank=True)

    def __init__(self, *args, **kwargs):
        super(Account, self).__init__(*args, **kwargs)
        self._original_fields = dict([(field.attname, getattr(self, field.attname))
            for field in self._meta.local_fields if not isinstance(field, models.ForeignKey)])

    def save(self, *args, **kwargs):
        if self.id:
            for field in self._meta.local_fields:
                if not isinstance(field, models.ForeignKey) and\
                   self._original_fields['username'] != getattr(self,
                                                                'username'):
                    if (self._original_fields['username'] is not None):
                        r.delete('username:' + self._original_fields['username'])
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['username'] is not None) and\
                   self._original_fields['pin'] != getattr(self, 'pin') and\
                   len(r.hgetall('username:' + getattr(self, 'username'))) > 0:
                    r.hset('username:' + getattr(self, 'username'),
                           'password', getattr(self, 'pin'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['username'] is not None) and\
                   self._original_fields['ip_address'] != getattr(self, 'ip_address') and\
                   len(r.hgetall('username:' + getattr(self, 'username'))) > 0:
                    r.hset('username:' + getattr(self, 'username'),
                           'ipAddress', getattr(self, 'ip_address'))


        try:
            account = kwargs.pop('account')
            # only for creating new account
            is_delete = False
        except KeyError:
            # doing soft delete account, update operation
            is_delete = True

        super(Account, self).save(*args, **kwargs)

        # add all product fees whenever a new account is created
        # and set fee for Axes at most 300, others at most 100.

        # check adm/pp views. pass account None or account var via args/kwargs
        # fee <= calculate from maximum child fee

        if is_delete == False:
            if account is None:
                for product in Product.objects.filter(is_active=True):
                    parent_max_fee = product.admin_fee - product.biller_fee
                    if settings.OPERATOR_FEE <= parent_max_fee:
                        fee = settings.OPERATOR_FEE
                        child_max_fee = parent_max_fee - fee
                    else:
                        fee = parent_max_fee
                        child_max_fee = 0

                    if product.add_auto == True:
                        ProductFee.objects.create(
                            parent=account, child=self, product=product,
                            fee=fee, child_max_fee=child_max_fee,
                            date_activated=timezone.now())
            else:
                for product in Product.objects.filter(is_active=True):
                    try:
                        pf = ProductFee.objects.get(child=account,
                                                    product=product)
                        if settings.ACCOUNT_FEE <= pf.child_max_fee:
                            fee = settings.ACCOUNT_FEE
                            child_max_fee = pf.child_max_fee - fee
                        else:
                            fee = pf.child_max_fee
                            child_max_fee = 0

                        if product.add_auto == True:
                            ProductFee.objects.create(
                                parent=account, child=self, product=product,
                                fee=fee, child_max_fee=child_max_fee,
                                date_activated=timezone.now())
                    except ProductFee.DoesNotExist:
                        pass


    class Meta:
        permissions = (
            ('axes_create_account', 'Allowed to create account'),
            ('axes_read_account', 'Allowed to read account'),
            ('axes_update_account', 'Allowed to update account'),
            ('axes_delete_account', 'Allowed to delete account'),
            ('axes_multi_level_account',
             'Allowed to view multi level account'),
            ('axes_create_fee_administration',
             'Allowed to create fee administration'),
            ('axes_read_fee_administration',
             'Allowed to read fee administration'),
            ('axes_update_fee_administration',
             'Allowed to update fee administration'),
            ('axes_delete_fee_administration',
             'Allowed to delete fee administration'),
            ('axes_multi_level_fee_administration',
             'Allowed to view multi level fee administration'),
            ('axes_create_balance', 'Allowed to create balance'),
            ('axes_read_balance', 'Allowed to read balance'),
            ('axes_update_balance', 'Allowed to update balance'),
            ('axes_delete_balance', 'Allowed to delete balance'),
            ('axes_multi_level_balance',
             'Allowed to view multi level balance'),
            ('axes_create_admin_account', 'Allowed to create admin account'),
            ('axes_read_admin_account', 'Allowed to read admin account'),
            ('axes_update_admin_account', 'Allowed to update admin account'),
            ('axes_delete_admin_account', 'Allowed to delete admin account'),
            ('axes_multi_level_admin_account',
             'Allowed to view multi level admin account'),
            ('axes_create_topup_approval', 'Allowed to topup approval'),
            ('axes_read_topup_approval', 'Allowed to topup approval'),
            ('axes_update_topup_approval', 'Allowed to topup approval'),
            ('axes_delete_topup_approval', 'Allowed to topup approval'),
            ('axes_multi_level_topup_approval',
             'Allowed to view multi level topup approval'),

            # balance summary
            ('axes_create_balance_summary',
                'Allowed to create balance summary'),
            ('axes_read_balance_summary', 'Allowed to read balance summary'),
            ('axes_update_balance_summary',
                'Allowed to update balance summary'),
            ('axes_delete_balance_summary',
                'Allowed to delete balance summary'),
            ('axes_multi_level_balance_summary',
                'Allowed to view multi level balance summary'),
        )

    def get_absolute_url(self):
        return reverse('account-list')

    def __unicode__(self):
        #return u'%d' % self.id
        return self.name


class Role(models.Model):
    role = models.OneToOneField(Group)
    date_created = models.DateTimeField(auto_now_add=True, auto_now=True)
    is_active = models.BooleanField()
    is_staff = models.BooleanField()

    class Meta:
        permissions = (
            ('axes_create_user_role', 'Allowed to create user role'),
            ('axes_read_user_role', 'Allowed to read user role'),
            ('axes_update_user_role', 'Allowed to update user role'),
            ('axes_delete_user_role', 'Allowed to delete user role'),
            ('axes_multi_level_user role',
             'Allowed to view multi level user role'),
            ('axes_create_admin_role', 'Allowed to create admin role'),
            ('axes_read_admin_role', 'Allowed to read admin role'),
            ('axes_update_admin_role', 'Allowed to update admin role'),
            ('axes_delete_admin_role', 'Allowed to delete admin role'),
            ('axes_multi_level_admin role',
             'Allowed to view multi level admin role'),
        )

    def get_absolute_url(self):
        return reverse('account-list')

    def __unicode__(self):
        return self.role.name


class AxesUser(AbstractBaseUser, PermissionsMixin):
    username = models.CharField(_('username'), max_length=30, unique=True,
        #help_text=_('Required. 30 characters or fewer. Letters, numbers and '
        #            '@/./+/-/_ characters'),
        validators=[
            validators.RegexValidator(re.compile('^[\w.@+-]+$'),
                                      _('Enter a valid username.'), 'invalid')
        ])
    full_name = models.CharField(_('full name'), max_length=100)
    phone = models.CharField(_('phone number'), max_length=40, unique=True)
    email = models.EmailField(_('email address'), max_length=254, unique=True)
    is_staff = models.BooleanField(_('staff status'), default=False,
        help_text=_('Designates whether the user can log into this admin '
                    'site.'))
    is_active = models.BooleanField(_('active'), default=True,
        help_text=_('Designates whether this user should be treated as '
                    'active. Unselect this instead of deleting accounts.'))
    date_joined = models.DateTimeField(_('date joined'), default=timezone.now)
    is_level_2 = models.BooleanField(_('authentication level 2'), default=False)
    account = models.ForeignKey(Account, null=True, blank=True)

    USERNAME_FIELD = 'username'
    REQUIRED_FIELDS = ['full_name', 'phone', 'email']

    objects = UserManager()

    class Meta:
        ordering = ["date_joined"]

    def get_full_name(self):
        return self.fullname

    def get_short_name(self):
        return self.fullname


class AxesUserManager(BaseUserManager):
    def _create_user(self, username, full_name, email, phone, password,
                     is_staff, **extra_fields):
        now = django.utils.timezone.now()
        if not username:
            raise ValueError('Username must be set')
        if not full_name:
            raise ValueError('Full name must be set')
        if not email:
            raise ValueError('Email must be set')
        if not phone:
            raise ValueError('Phone must be set')
        username = self.normalize_username(username)
        full_name = self.normalize_full_name(full_name)
        email = self.normalize_email(email)
        phone = self.normalize_phone(phone)

        user = self.model(username=username, full_name=full_name, email=email,
                          phone=phone, is_staff=is_staff, is_active=True,
                          is_superuser=False, last_login=now, date_joined=now,
                          **extra_fields)
        user.set_password(password)
        user.is_level_2 = False
        user.save(using=self._db)
        return user

    def create_user(self, username, full_name, email, phone, password,
                    **extra_fields):
        return self._create_user(username, full_name, email, phone, password,
                                 False, **extra_fields)

    def create_superuser(self, username, full_name, email, phone, password,
                         **extra_fields):
        return self._create_user(username, full_name, email, phone, password,
                                 True, **extra_fields)


class CommonAccess(models.Model):
    ip_address = models.IPAddressField(verbose_name='IP Address', null=True)
    is_blocked = models.BooleanField(default=False)
    attempt_time = models.DateTimeField(auto_now_add=True, auto_now=True)
    failures_since_start = models.PositiveIntegerField(
        verbose_name='Failed Logins', default=0)
    is_active = models.BooleanField()

    class Meta:
        ordering = ['-attempt_time']


class LoginAttempt(models.Model):
    ip_address = models.ForeignKey(CommonAccess)
    username = models.CharField(max_length=30)
    password = models.CharField(max_length=30)
    user_agent = models.CharField(max_length=255)
    http_accept = models.CharField(verbose_name='HTTP Accept', max_length=1025)
    attempt_time = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-attempt_time']

    def __unicode__(self):
        return u'%s | %15s | %15s ' % (self.attempt_time, self.username,
                                       self.password)


class Biller(models.Model):
    name = models.CharField(max_length=30, unique=True)
    # code should not be displayed in biller form. and auto increment
    code = models.CharField(max_length=15, unique=True)
    ip_address = models.GenericIPAddressField(_('IP Address'), null=True,
                                              blank=True)
    port = models.IntegerField(null=True, blank=True)
    direct_payment = models.BooleanField(_('Direct Payment (Khusus Postpaid, Inquiry dan Payment digabung menjadi satu)'), default=False)
    has_counter = models.BooleanField(_("Memiliki loket"), default=False)
    has_sa = models.BooleanField(_("Memiliki SA"),
                                 default=False)
    merchant_code = models.CharField(_('Merchant Code'), max_length=15,
                                     null=True, blank=True)
    merchant_number = models.CharField(_('Merchant Number'), max_length=15,
                                       null=True, blank=True)
    terminal = models.CharField(max_length=10, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    url = models.CharField(max_length=100, null=True, blank=True)
    date_created = models.DateTimeField(auto_now_add=True, auto_now=True)
    username = models.CharField(_('Username'), max_length=30, null=True,
        blank=True,
        validators=[
            validators.RegexValidator(re.compile('^[\w.@+-]+$'),
                                      _('Enter a valid username.'), 'invalid')
        ])
    password = models.CharField(max_length=120, null=True, blank=True)

    def __init__(self, *args, **kwargs):
        super(Biller, self).__init__(*args, **kwargs)
        self._original_fields = dict([(field.attname, getattr(self, field.attname))
            for field in self._meta.local_fields if not isinstance(field, models.ForeignKey)])

    def save(self, *args, **kwargs):
        if self.id:
            for field in self._meta.local_fields:
                if not isinstance(field, models.ForeignKey) and\
                   self._original_fields['code'] != getattr(self, 'code'):
                    if (self._original_fields['code'] is not None):
                        r.delete('biller_map:' + self._original_fields['code'])
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['code'] is not None) and\
                   self._original_fields['username'] != getattr(self, 'username') and\
                   len(r.hgetall('biller_map:' + getattr(self, 'code'))) > 0:
                    r.hset('biller_map:' + getattr(self, 'code'), 'username', getattr(self, 'username'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['code'] is not None) and\
                   self._original_fields['password'] != getattr(self, 'password') and\
                   len(r.hgetall('biller_map:' + getattr(self, 'code'))) > 0:
                    r.hset('biller_map:' + getattr(self, 'code'), 'password', getattr(self, 'password'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['code'] is not None) and\
                   self._original_fields['merchant_code'] != getattr(self, 'merchant_code') and\
                   len(r.hgetall('biller_map:' + getattr(self, 'code'))) > 0:
                    r.hset('biller_map:' + getattr(self, 'code'), 'merchant_code', getattr(self, 'merchant_code'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['code'] is not None) and\
                   self._original_fields['merchant_number'] != getattr(self, 'merchant_number') and\
                   len(r.hgetall('biller_map:' + getattr(self, 'code'))) > 0:
                    r.hset('biller_map:' + getattr(self, 'code'), 'merchant_number', getattr(self, 'merchant_number'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['code'] is not None) and\
                   self._original_fields['terminal'] != getattr(self, 'terminal') and\
                   len(r.hgetall('biller_map:' + getattr(self, 'code'))) > 0:
                    r.hset('biller_map:' + getattr(self, 'code'), 'terminal', getattr(self, 'terminal'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['code'] is not None) and\
                   self._original_fields['ip_address'] != getattr(self, 'ip_address') and\
                   len(r.hgetall('biller_map:' + getattr(self, 'code'))) > 0:
                    r.hset('biller_map:' + getattr(self, 'code'), 'ip_address', getattr(self, 'ip_address'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['code'] is not None) and\
                   self._original_fields['port'] != getattr(self, 'port') and\
                   len(r.hgetall('biller_map:' + getattr(self, 'code'))) > 0:
                    r.hset('biller_map:' + getattr(self, 'code'), 'port', getattr(self, 'port'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['code'] is not None) and\
                   self._original_fields['url'] != getattr(self, 'url') and\
                   len(r.hgetall('biller_map:' + getattr(self, 'code'))) > 0:
                    r.hset('biller_map:' + getattr(self, 'code'), 'url', getattr(self, 'url'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['code'] is not None) and\
                   self._original_fields['direct_payment'] != getattr(self, 'direct_payment') and\
                   len(r.hgetall('biller_map:' + getattr(self, 'code'))) > 0:
                    r.hset('biller_map:' + getattr(self, 'code'), 'directPayment', getattr(self, 'direct_payment'))
                if not isinstance(field, models.ForeignKey) and\
                   self._original_fields['direct_payment'] != getattr(self, 'direct_payment'):
                    r.set('payment_type:' + getattr(self, 'code'), getattr(self, 'direct_payment'))

        super(Biller, self).save(*args, **kwargs)

    class Meta:
        ordering = ['id','-is_active', 'name']
        permissions = (
            ('axes_create_biller', 'Allowed to create biller'),
            ('axes_read_biller', 'Allowed to read biller'),
            ('axes_update_biller', 'Allowed to update biller'),
            ('axes_delete_biller', 'Allowed to delete biller'),
            ('axes_multi_level_biller',
             'Allowed to view multi level biller'),
        )

    def __unicode__(self):
        return self.name


class BillerMappingCode(models.Model):
    biller = models.ForeignKey(Biller)
    code = models.CharField(max_length=15)
    description = models.TextField(null=True, blank=True)

    def __unicode__(self):
        return "%s - %s" % (self.code, self.description)


class ProductGroup(models.Model):
    name = models.CharField(max_length=30, unique=True)

    class Meta:
        ordering = ['name']

    def delete(self, *args, **kwargs):
        """Update any product which belongs to the selected group to NULL
           before delete the group."""
        pg = ProductGroup.objects.get(pk=self.id)
        products = Product.objects.filter(group=pg)
        for p in products:
            p.group = None
            p.save()

        super(ProductGroup, self).delete(*args, **kwargs)

    def __unicode__(self):
        return self.name


class Product(models.Model):
    # TODO: change to smallintegerfield
    INCLUDED_FEE = 'IN'
    EXCLUDED_FEE = 'EX'
    PAYMENT_FEE_TYPE = (
        (EXCLUDED_FEE, 'Excluded Fee'),
        (INCLUDED_FEE, 'Included Fee'),
    )

    POSTPAID = 1
    PREPAID = 2
    PAYMENT_TYPE = (
        (POSTPAID, 'Postpaid'),
        (PREPAID, 'Prepaid'),
    )

    name = models.CharField(max_length=75, unique=True)
    biller = models.ForeignKey(Biller)
    code = models.CharField(max_length=10)
    internal_code = models.CharField(max_length=10, unique=True)
    admin_fee = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    biller_fee = models.DecimalField(max_digits=15, decimal_places=2,
                                     default=0)
    type = models.CharField(max_length=2, choices=PAYMENT_FEE_TYPE,
                            default=EXCLUDED_FEE)
    is_active = models.BooleanField(default=True)
    payment = models.SmallIntegerField(choices=PAYMENT_TYPE,
                                       default=POSTPAID)
    denom = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    group = models.ForeignKey(ProductGroup, null=True, blank=True)
    add_auto = models.BooleanField(_("Add to Product Fee automatically"),
                                   default=True)

    def __init__(self, *args, **kwargs):
        super(Product, self).__init__(*args, **kwargs)
        self._original_fields = dict([(field.attname, getattr(self, field.attname))
            for field in self._meta.local_fields if not isinstance(field, models.ForeignKey)])

    def save(self, *args, **kwargs):
        if self.id:
            for field in self._meta.local_fields:
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['internal_code'] is not None) and\
                   self._original_fields['internal_code'] != getattr(self, 'internal_code'):
                    if ('product_map:' + self._original_fields['internal_code'] is not None):
                        r.delete('product_map:' + self._original_fields['internal_code'])
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['internal_code'] is not None) and\
                   self._original_fields['code'] != getattr(self, 'code') and\
                   len(r.hgetall('product_map:' + getattr(self, 'internal_code'))) > 0:
                    r.hset('product_map:' + getattr(self, 'internal_code'), 'productCode', getattr(self, 'code'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['internal_code'] is not None) and\
                   self._original_fields['payment'] != getattr(self, 'payment') and\
                   len(r.hgetall('product_map:' + getattr(self, 'internal_code'))) > 0:
                    r.hset('product_map:' + getattr(self, 'internal_code'), 'paymentType', getattr(self, 'payment'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['internal_code'] is not None) and\
                   self._original_fields['denom'] != getattr(self, 'denom') and\
                   len(r.hgetall('product_map:' + getattr(self, 'internal_code'))) > 0:
                    r.hset('product_map:' + getattr(self, 'internal_code'), 'denom', getattr(self, 'denom'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['internal_code'] is not None) and\
                   self._original_fields['type'] != getattr(self, 'type') and\
                   len(r.hgetall('product_map:' + getattr(self, 'internal_code'))) > 0:
                    r.hset('product_map:' + getattr(self, 'internal_code'), 'feeType', getattr(self, 'type'))
                if not isinstance(field, models.ForeignKey) and\
                   (self._original_fields['internal_code'] is not None) and\
                   self._original_fields['admin_fee'] != getattr(self, 'admin_fee') and\
                   len(r.hgetall('product_map:' + getattr(self, 'internal_code'))) > 0:
                    r.hset('product_map:' + getattr(self, 'internal_code'), 'feeAdmin', getattr(self, 'admin_fee'))
        super(Product, self).save(*args, **kwargs)

    class Meta:
        ordering = ['name']
        #ordering = ['id','-is_active', 'name']
        permissions = (
            ('axes_create_product', 'Allowed to create product'),
            ('axes_read_product', 'Allowed to read product'),
            ('axes_update_product', 'Allowed to update product'),
            ('axes_delete_product', 'Allowed to delete product'),
            ('axes_multi_level_product',
             'Allowed to view multi level product'),
        )

    def __unicode__(self):
        return self.name


class Counter(models.Model):
    name = models.CharField(max_length=20)
    username = models.CharField(max_length=50)
    password = models.CharField(max_length=50)
    product = models.ForeignKey(Product)
    biller = models.ForeignKey(Biller)


class AdminProductFee(models.Model):
    product = models.OneToOneField(Product)
    fee = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    date_activated = models.DateTimeField(blank=True, null=True)

    def __unicode__(self):
        return self.product.name


class ProductFee(models.Model):
    parent = models.ForeignKey(Account, related_name='apf_parent',
                               null=True, blank=True)
    product = models.ForeignKey(Product)
    child = models.ForeignKey(Account, related_name='apf_child')
    # maximum fee for a child
    # call it child fee
    child_max_fee = models.DecimalField(max_digits=15, decimal_places=2,
                                        default=0)
    # fee is the amount money taken by parent when a child does transaction
    # call it parent fee
    fee = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    date_activated = models.DateTimeField(blank=True, null=True)

    class Meta:
        unique_together = ('parent', 'product', 'child')
        permissions = (
            ('axes_create_product_fee', 'Allowed to create product fee'),
            ('axes_read_product_fee', 'Allowed to read product fee'),
            ('axes_update_product_fee', 'Allowed to update product fee'),
            ('axes_delete_product_fee', 'Allowed to delete product fee'),
            ('axes_multi_level_product_fee',
             'Allowed to view multi level product fee'),
        )

    def __unicode__(self):
        if self.parent is None:
            return "None > %s > %s" % (self.product.name, self.child.name)
        else:
            return "%s > %s > %s" % \
                (self.parent.name, self.product.name, self.child.name)


class Menu(MPTTModel):
    name = models.CharField(max_length=20, unique=True)
    parent = TreeForeignKey('self', null=True,
                            blank=True, related_name="child")
    url = models.CharField(max_length=50, blank=True, null=True)
    is_level_2 = models.BooleanField(default=False)

    class Meta:
        permissions = (
            ('axes_create_user_menu', 'Allowed to create user menu'),
            ('axes_read_user_menu', 'Allowed to read user menu'),
            ('axes_update_user_menu', 'Allowed to update user menu'),
            ('axes_delete_user_menu', 'Allowed to delete user menu'),
            ('axes_multi_level_user_menu',
             'Allowed to view multi level user menu'),
        )

    def __unicode__(self):
        return self.name


class AdminMenu(MPTTModel):
    name = models.CharField(max_length=20, unique=True)
    parent = TreeForeignKey('self', null=True,
                            blank=True, related_name="child")
    url = models.CharField(max_length=50, blank=True, null=True)
    is_level_2 = models.BooleanField(default=False)

    class Meta:
        permissions = (
            ('axes_create_admin_menu', 'Allowed to create user menu'),
            ('axes_read_admin_menu', 'Allowed to read user menu'),
            ('axes_update_admin_menu', 'Allowed to update user menu'),
            ('axes_delete_admin_menu', 'Allowed to delete user menu'),
            ('axes_multi_level_admin_menu',
             'Allowed to view multi level user menu'),
        )

    def __unicode__(self):
        return self.name


class SpecialMenu(models.Model):
    name = models.CharField(max_length=20, unique=True)
    url = models.CharField(max_length=50, blank=True, null=True)
    is_level_2 = models.BooleanField(default=False)


class TopUp(models.Model):
    TOPUP = 1
    TOPDOWN = 2

    requested_account_by = models.ForeignKey(
        Account, related_name='requested_account_by')
    requested_user_by = models.ForeignKey(AxesUser)
    requested_account_to = models.ForeignKey(
        Account, related_name='requested_account_to', blank=True, null=True)
    requested_date = models.DateTimeField(auto_now_add=True, auto_now=True)
    amount = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    type = models.SmallIntegerField()
    description = models.TextField()
    is_active = models.BooleanField(default=True)


class TopUpHistory(models.Model):
    # action
    APPROVE = 'approve'
    REJECT = 'reject'

    topup = models.OneToOneField(TopUp)
    updated_by = models.ForeignKey(AxesUser)
    updated_date = models.DateTimeField(auto_now_add=True, auto_now=True)
    action = models.SmallIntegerField()


class FeeHistory(models.Model):
    #status
    SUCCESS = 1
    FAIL = 0

    # type
    FEE2BALANCE = 1

    updated_date = models.DateTimeField(auto_now_add=True, auto_now=True)
    updated_account_by = models.ForeignKey(Account)
    updated_user_by = models.ForeignKey(AxesUser)
    type = models.SmallIntegerField()
    amount = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    status = models.SmallIntegerField()
    note = models.CharField(max_length=50, blank=True, null=True)


class Mutasi(models.Model):
    # status
    SUCCESS = 0
    FAIL = 1

    TRX_STATUS = (
        (SUCCESS, '0'),
        (FAIL, '1'),
    )

    # type
    ALL = 0
    TOPUP_REQUEST = 1
    TOPUP_APPROVED = 2
    TOPDOWN_REQUEST = 3
    TOPDOWN_APPROVED = 4
    FEE2BALANCE = 5
    TRANSACTION = 6

    TRX_TYPE = (
        (ALL, 'Semua'),
        (TOPUP_REQUEST, 'TopUp Request'),
        (TOPUP_APPROVED, 'TopUp Approved'),
        (TOPDOWN_REQUEST, 'TopDown Request'),
        (TOPDOWN_APPROVED, 'TopDown Approved'),
        (FEE2BALANCE, 'Fee To Balance'),
        (TRANSACTION, 'Transaction'),
    )

    created_user_by = models.CharField(max_length=30)
    transaction_id = models.CharField(max_length=8, null=True)
    product = models.CharField(max_length=75, null=True)
    date_created = models.DateTimeField(auto_now_add=True, auto_now=True)
    transaction_type = models.SmallIntegerField(choices=TRX_TYPE)
    customer_id = models.CharField(max_length=30, null=True)
    debit = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    credit = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    transaction_status = models.SmallIntegerField(choices=TRX_STATUS,
                                                  default=SUCCESS)
    note = models.TextField(blank=True, null=True)

    class Meta:
        abstract = True


class MutasiBalance(Mutasi):
    account_by = models.ForeignKey(Account, blank=True, null=True,
                                   related_name='mutasibalance_by')
    account_to = models.ForeignKey(Account, blank=True, null=True,
                                   related_name='mutasibalance_to')
    balance = models.DecimalField(max_digits=15, decimal_places=2)

    class Meta:
        permissions = (
            ('axes_create_mutasi_balance', 'Allowed to create mutasi balance'),
            ('axes_read_mutasi_balance', 'Allowed to read mutasi balance'),
            ('axes_update_mutasi_balance', 'Allowed to update mutasi balance'),
            ('axes_delete_mutasi_balance', 'Allowed to delete mutasi balance'),
            ('axes_multi_level_mutasi_balance',
             'Allowed to view multi level mutasi balance'),
        )


class MutasiFee(Mutasi):
    account_by = models.ForeignKey(Account, blank=True, null=True,
                                   related_name='mutasifee_by')
    account_to = models.ForeignKey(Account, blank=True, null=True,
                                   related_name='mutasifee_to')
    fee = models.DecimalField(max_digits=15, decimal_places=2)

    class Meta:
        permissions = (
            ('axes_create_mutasi_fee', 'Allowed to create mutasi fee'),
            ('axes_read_mutasi_fee', 'Allowed to read mutasi fee'),
            ('axes_update_mutasi_fee', 'Allowed to update mutasi fee'),
            ('axes_delete_mutasi_fee', 'Allowed to delete mutasi fee'),
            ('axes_multi_level_mutasi_fee',
             'Allowed to view multi level mutasi fee'),
        )


class Operator(models.Model):
    fee = models.DecimalField(max_digits=14, decimal_places=2)


class Transaction(models.Model):
    # Status
    ALL = 0
    FAIL = 1
    PENDING = 2
    SUCCESS = 3

    STATUS = (
        (ALL, 'Semua'),
        (SUCCESS, 'Success'),
        (FAIL, 'Fail'),
        (PENDING, 'Pending'),
    )

    transaction_id = models.CharField(max_length=8, null=True)
    transaction_ref_id = models.CharField(max_length=20, null=True)
    account = models.ForeignKey(Account, blank=False, null=False)
    product = models.ForeignKey(Product, blank=False, null=False)
    bill_number = models.CharField(max_length=30, blank=False, null=False)
    status = models.SmallIntegerField(choices=STATUS)
    amount = models.DecimalField(max_digits=15, decimal_places=2)
    result_code = models.CharField(max_length=6, null=True, blank=True)
    note = models.CharField(max_length=200, null=True, blank=True)
    bit_61 = models.CharField(max_length=2000, null=False, blank=False)
    bit_48 = models.CharField(max_length=2000, null=False, blank=False)
    timestamp = models.DateTimeField(auto_now_add=False, auto_now=False)

    class Meta:
        permissions = (
            ('axes_create_transaction', 'Allowed to create transaction'),
            ('axes_read_transaction', 'Allowed to read transaction'),
            ('axes_update_transaction', 'Allowed to update transaction'),
            ('axes_delete_transaction', 'Allowed to delete transaction'),
            ('axes_multi_level_transaction',
             'Allowed to view multi level transaction'),

            ('axes_create_transaction_summary',
             'Allowed to create transaction summary'),
            ('axes_read_transaction_summary',
             'Allowed to read transaction summary'),
            ('axes_update_transaction_summary',
             'Allowed to update transaction summary'),
            ('axes_delete_transaction_summary',
             'Allowed to delete transaction summary'),
            ('axes_multi_level_transaction_summary',
             'Allowed to view multi level transaction summary'),
        )

    def __unicode__(self):
        return self.transaction_id

    def alter_bill_number(self):
        # for Artajasa PLN
        if '#' in self.bill_number:
            return self.bill_number.split('#')[1]
        return self.bill_number

    def alter_product(self):
        # for Artajasa PLN
        if '#' in self.bill_number:
            status = self.bill_number.split('#')[0]
            if int(status) == 1:
                return "PLN Postpaid"
            elif int(status) == 2:
                return "PLN Prepaid"
            elif int(status) == 3:
                return "PLN Nontaglis"
            else:
                return
        return self.product.name


class LoyalCustomer(models.Model):
    name = models.CharField(max_length=100)
    customer_id = models.CharField(_('Customer ID'), max_length=20, null=True)
    account = models.ForeignKey(Account)
    product = models.ForeignKey(Product)
    frequency = models.IntegerField(default=0)

    class Meta:
        unique_together = ('customer_id', 'product', 'account')
        permissions = (
            ('axes_create_loyalcustomer', 'Allowed to create loyal customer'),
            ('axes_read_loyalcustomer', 'Allowed to read loyal customer'),
            ('axes_update_loyalcustomer', 'Allowed to update loyal customer'),
            ('axes_delete_loyalcustomer', 'Allowed to delete loyal customer'),
            ('axes_multi_level_loyalcustomer', 'Allowed to view multi level loyal customer'),
        )

    def __unicode__(self):
        return "%s - %s" % (self.name, self.customer_id)


class ErrorCode(models.Model):
    biller = models.ForeignKey(Biller)
    code = models.CharField(max_length=6)
    description = models.CharField(max_length=300)

    def __unicode__(self):
        return "%s - %s" % (self.code, self.description)


class NewsFeed(models.Model):
    title = models.CharField(max_length=100)
    text = models.TextField()
    author = models.ForeignKey(AxesUser, related_name='author')
    edited_by = models.ForeignKey(AxesUser, null=True,
                                  related_name='edited_by')
    is_published = models.BooleanField(default=True)
    created_time = models.DateTimeField(auto_now_add=True, auto_now=False)
    edited_time = models.DateTimeField(auto_now_add=False, auto_now=True)

    def __unicode__(self):
        return "%s - %s" % (self.title, self.author)

    class Meta:
        permissions = (
            ('axes_create_newsfeed', 'Allowed to create newsfeed'),
            ('axes_read_newsfeed', 'Allowed to read newsfeed'),
            ('axes_update_newsfeed', 'Allowed to update newsfeed'),
            ('axes_delete_newsfeed', 'Allowed to delete newsfeed'),
        )


class Configuration(models.Model):
    config = models.CharField(max_length=50)
    description = models.CharField(max_length=300, blank=True, null=True)
    value = models.CharField(max_length=50)
    default = models.CharField(max_length=50)
    need_restart = models.BooleanField(default=True)

    def __unicode__(self):
        return "%s -> %s" % (self.config, self.value)

    class Meta:
        permissions = (
            ('axes_create_configuration', 'Allowed to create configuration'),
            ('axes_read_configuration', 'Allowed to read configuration'),
            ('axes_update_configuration', 'Allowed to update configuration'),
            ('axes_delete_configuration', 'Allowed to delete configuration'),
        )
