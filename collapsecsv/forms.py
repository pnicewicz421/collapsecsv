from django import forms

class UploadFileForm(forms.Form):
    #filename = forms.CharField(max_length=500)
    filename = forms.FileField(label='Select a CSV 4.1 file (hashed for RHY)',)

    #email = forms.CharField(label='Enter the email you want your 5.0 to be sent',)