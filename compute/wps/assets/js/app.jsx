import React, { Component } from 'react';
import { withRouter } from 'react-router';
import { 
  Route,
  Link,
  Switch,
} from 'react-router-dom';

import axios from 'axios';

import Login from './login.jsx';
import LoginMPC from './login_mpc.jsx';
import LoginOAuth2 from './login_oauth2.jsx';
import CreateAccount from './create_account.jsx';
import Servers from './servers.jsx';
import Processes from './processes.jsx';
import User from './user.jsx';

class App extends Component {
  constructor(props) {
    super(props);

    this.state = {
      logged: false,
    }

    this.history = props.history;
  }

  handleAction(e) {
    const target = e.target;
    const name = target.name;

    switch (name) {
      case "login": {
        this.history.push('/wps/debug/login');

        break;
      }
      case "logout": {
        const logoutURL = location.origin + '/auth/logout';

        axios.get(logoutURL)
          .then(res => {
            this.setState({ logged: false });
          })
          .catch(err => {
            console.log(err);
          });

        break;
      }
      case "create": {
        this.history.location('/wps/debug/create');

        break;
      }
      default: break;
    }
  }

  handleLogin(e) {
    this.setState({ logged: true });

    this.history.push('/wps/debug/user');
  }

  render() {
    return (
      <div>
        <nav>
          <ul>
            <li><Link to="/wps/debug">Home</Link></li>
            {this.state.logged &&
              <li><Link to="/wps/debug/user">Profile</Link></li>
            }
          </ul>
          <ul>
            <li>
              {this.state.logged ? 
                  <button name="logout" onClick={(e) => this.handleAction(e)} >Logout</button> :
                  <button name="login" onClick={(e) => this.handleAction(e)} >Login</button>
              }
            </li>
            {!this.state.logged &&
              <li>
                <button name="create" onClick={(e) => this.handleAction(e)} >Create Account</button>
              </li>
            }
          </ul>
        </nav>
        <Switch>
          <Route exact path='/wps/debug/' component={Home} />
          <Route path='/wps/debug/create/' component={CreateAccount} />
          <Route path='/wps/debug/user/' component={User} />
          <Route exact path='/wps/debug/login/' component={() => <Login handleLogin={(e) => this.handleLogin(e)} />} />
          <Route path='/wps/debug/login/mpc/' component={LoginMPC} />
          <Route path='/wps/debug/login/oauth2' component={LoginOAuth2} />
          <Route exact path='/wps/debug/servers/' component={Servers} />
          <Route path='/wps/debug/servers/:server_id' component={Processes} />
          <Route component={NotFound} />
        </Switch>
      </div>
    )
  }
}

const Home = () => <h1>Hello from Home!</h1>
const NotFound = () => <h1>404 Page is not found!</h1>

export default withRouter(App)
