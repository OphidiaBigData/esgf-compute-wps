# -*- coding: utf-8 -*-
# Generated by Django 1.10.6 on 2017-03-31 16:32
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('wps', '0006_multi_backends'),
    ]

    operations = [
        migrations.AlterField(
            model_name='process',
            name='identifier',
            field=models.CharField(max_length=128, unique=True),
        ),
    ]