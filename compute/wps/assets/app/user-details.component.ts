import { Component, OnInit } from '@angular/core';

import { WPSResponse } from './wps.service';
import { AuthService, User } from './auth.service';
import { NotificationService } from './notification.service';

declare var jQuery: any;

@Component({
  selector: 'user-details',
  templateUrl: './user-details.component.html',
  styleUrls: ['./forms.css']
})
export class UserDetailsComponent { 
  model: User = {} as User;
  mpc: User = {} as User;
  error: boolean = false;
  errorMessage: string;

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.authService.user$.subscribe((value: User) => {
      this.model = value;
    });
  }

  onSubmit(form: any) {
    let user = {} as User;

    for (let control in form.controls) {
      if (form.controls[control].dirty) {
        user[control] = form.controls[control].value;
      }
    }

    this.authService.update(user)
      .then(response => {
        if (response.status === 'success') {
          this.model = response.data as User;

          this.notificationService.message('Successfully updated user details');
        } else {
          this.notificationService.error('Failed to update account details');
        }
      });
  }

  onRegenerateKey() {
    this.authService.regenerateKey(this.model)
      .then(response => {
        if (response.status === 'success') {
          this.model.api_key = response.data.api_key;

          this.notificationService.message('Successfully generated a new API key');
        } else {
          this.notificationService.error('Failed to generate a new API key'); 
        }
      });
  }

  onOAuth2() {
    this.authService.oauth2(this.model.openid)
      .then(response => {
        if (response.status === 'success') {
          window.location.replace(response.data.redirect);
        } else {
          this.notificationService.error(`OAuth2 failed with server error "${response.error}"`);
        }
      });
  }

  onMPCSubmit() {
    this.authService.myproxyclient(this.mpc)
      .then(response => {
        if (response.status === 'success') {
          jQuery('#myproxyclient').modal('hide');

          this.model.type = response.data.type;

          this.model.api_key = response.data.api_key;

          this.notificationService.message('Successfully authenticated using ESGF MyProxyClient');
        } else {
          this.notificationService.error(`MyProxyClient failed with server error "${response.error}"`);
        }
      });
  }
}
