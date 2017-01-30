from __future__ import unicode_literals

from django.db import models

class FileUpload(models.Model):
    #file = models.CharField(max_length=500)
    filename = models.FileField(upload_to = 'files/') #don't need upload_to if MEDIA_ROOT is defined in settings.py
    #email = models.CharField(max_length=100, default="rhymis@acf.hhs.gov")
    
class ZIPStructure(models.Model): #This model tracks the directories and files that are extracted
    ZIPDirectory = models.TextField