# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-04-25 20:30
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('wps', '0010_oauth_openid_textfield'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='oauth2',
            name='access_token',
        ),
        migrations.RemoveField(
            model_name='oauth2',
            name='expires_at',
        ),
        migrations.RemoveField(
            model_name='oauth2',
            name='refresh_token',
        ),
        migrations.RemoveField(
            model_name='oauth2',
            name='scope',
        ),
        migrations.RemoveField(
            model_name='oauth2',
            name='token_type',
        ),
        migrations.AddField(
            model_name='oauth2',
            name='token',
            field=models.TextField(default='no'),
            preserve_default=False,
        ),
    ]