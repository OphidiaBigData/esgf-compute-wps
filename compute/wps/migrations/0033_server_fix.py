# -*- coding: utf-8 -*-
# Generated by Django 1.11.7 on 2017-12-08 20:05
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('wps', '0032_process_toggle'),
    ]

    operations = [
        migrations.AlterField(
            model_name='server',
            name='host',
            field=models.CharField(max_length=128, unique=True),
        ),
    ]